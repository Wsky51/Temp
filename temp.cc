#!/usr/bin/env python3
# 把依赖的 proto 描述符显式写入 record（先写依赖 channel），确保 cyber_monitor 回放时能注册这些类型。
from google.protobuf import descriptor_pool


from modules.msg.basic_msgs.header_pb2 import Header
from modules.msg.basic_msgs.geometry_pb2 import Point3D, Point2D
from modules.msg.perception_msgs.dynamic_common_pb2 import *
from modules.msg.perception_msgs.perception_desensization_pb2 import DesensizationInfoObject

from cyber.python.cyber_py3 import cyber
from cyber.python.cyber_py3 import record
from cyber.proto.simple_pb2 import SimpleMessage
from cyber.proto.unit_test_pb2 import Chatter
from modules.msg.state_machine_msgs.top_state_pb2 import TopState
from modules.msg.basic_msgs.error_code_pb2 import StatusPb
from modules.msg.basic_msgs.header_pb2 import Header
from modules.msg.calib_msgs.calib_param_pb2 import SensorParameters

from enum import Enum
from typing import Optional, Set
import sys

from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.descriptor import Descriptor, FileDescriptor

# 兼容不同protobuf版本的DescriptorDatabase导入
try:
    from google.protobuf.descriptor_database import DescriptorDatabase
except ImportError:
    from google.protobuf.descriptor_pool import DescriptorDatabase
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import MessageFactory
from google.protobuf.descriptor_pb2 import FileDescriptorSet

# 复用之前的Status类定义
class StatusCode(Enum):
    OK = 0
    INVALID_ARGUMENT = 1
    IO_OR_DESCRIPTOR_FATAL = 2

class Status:
    def __init__(self, code: StatusCode, message: str = ""):
        self.code = code
        self.message = message

    @classmethod
    def Ok(cls):
        return cls(StatusCode.OK)

    @classmethod
    def InvalidArgument(cls, message: str):
        return cls(StatusCode.INVALID_ARGUMENT, message)

    @classmethod
    def IoOrDescriptorFatal(cls, message: str):
        return cls(StatusCode.IO_OR_DESCRIPTOR_FATAL, message)

    def ok(self) -> bool:
        return self.code == StatusCode.OK


# 定义DynamicContext类
class DynamicContext:
    def __init__(self):
        self.pool: Optional[DescriptorPool] = None
        self.factory: Optional[MessageFactory] = None

def BuildMinimalFDSFromType(pool, type_full_name: str) -> bytes:

    # 1. 根据类型全名获取Descriptor
    try:
        descriptor = pool.FindMessageTypeByName(type_full_name)
    except KeyError:
        # 类型不存在时返回False
        return None
    if descriptor is None:
        return None

    # 2. 获取对应的FileDescriptor
    file_desc: FileDescriptor = descriptor.file
    if file_desc is None:
        return None

    file_descriptor_set = FileDescriptorSet()
    processed_files: Set[str] = set()

    # 3. 递归收集所有依赖的文件描述符
    def collect_dependencies(current_file: FileDescriptor):
        if current_file is None:
            return

        file_name = current_file.name
        if file_name in processed_files:
            return

        processed_files.add(file_name)

        # 先递归处理所有依赖项
        for dep_file in current_file.dependencies:
            collect_dependencies(dep_file)

        # 将当前文件描述符转换为FileDescriptorProto并添加到集合
        file_proto = FileDescriptorProto()
        current_file.CopyToProto(file_proto)

        # 确保依赖列表正确（清除后重新添加，与C++逻辑一致）
        file_proto.dependency[:] = [dep.name for dep in current_file.dependencies]

        # 添加到FileDescriptorSet
        file_descriptor_set.file.append(file_proto)

    # 4. 从目标文件开始收集
    collect_dependencies(file_desc)

    return file_descriptor_set.SerializeToString()
    
def MakeDynamicContext(fds, ctx: Optional[DynamicContext]) -> Status:
    if ctx is None:
        return Status.InvalidArgument("ctx is null")
    
    # 1. 创建描述符数据库（修正后的DescriptorDatabase）
    db = DescriptorDatabase()
    
    # 2. 遍历FileDescriptorSet中的每个FileDescriptorProto并添加到数据库
    for fd_proto in fds.file:
        try:
            db.Add(fd_proto)
        except Exception as e:
            print(f"[ERROR] DescriptorDatabase Add failed for: {fd_proto.name}", file=sys.stderr)
            return Status.IoOrDescriptorFatal(f"db add failed: {fd_proto.name}, error: {str(e)}")
    
    # 3. 创建基于数据库的DescriptorPool
    pool = DescriptorPool(db)
    
    # 4. 创建动态消息工厂
    factory = MessageFactory(pool=pool)
    
    # 5. 赋值到DynamicContext
    ctx.pool = pool
    ctx.factory = factory
    
    print(f"[INFO] DynamicContext created with {len(fds.file)} files")
    return Status.Ok()

# 加载FileDescriptorSet的函数（复用之前的实现）
def LoadFileDescriptorSetFromFile(path: str, out: Optional[FileDescriptorSet]) -> Status:
    if out is None:
        print(f"[ERROR] Descriptor loader: out=nullptr", file=sys.stderr)
        return Status.InvalidArgument("out is null")
    
    try:
        with open(path, "rb") as f:
            out.ParseFromString(f.read())
    except FileNotFoundError:
        print(f"[ERROR] Cannot open desc file: {path}", file=sys.stderr)
        return Status.IoOrDescriptorFatal(f"open desc failed: {path}")
    except Exception as e:
        print(f"[ERROR] Parse FileDescriptorSet failed: {path}", file=sys.stderr)
        return Status.IoOrDescriptorFatal(f"parse desc failed: {path}, error: {str(e)}")
    
    print(f"[INFO] Loaded FileDescriptorSet: {path}")
    return Status.Ok()

def debug_print_descriptor_names(desc_bytes):
    f = FileDescriptorSet()
    f.ParseFromString(desc_bytes)
    print("DescriptorSet contains files in this order:")
    for pf in f.file:
        print(" -", pf.name)


def build_file_descriptor_set(fd):
    """
    修复：直接遍历FileDescriptor的dependencies属性，确保收集所有依赖
    """
    pool = descriptor_pool.Default()
    fset = FileDescriptorSet()
    seen = set()  # 记录已处理的FileDescriptor.name
    added = set() # 记录已添加到fset的文件名

    def collect_fd(fd_obj):
        """递归收集FileDescriptor及其依赖（post-order）"""
        if fd_obj.name in seen:
            return
        seen.add(fd_obj.name)
        
        # 先递归处理所有依赖的FileDescriptor
        for dep_fd in fd_obj.dependencies:
            collect_fd(dep_fd)
        
        # 再将当前FileDescriptor转为Proto并加入fset
        if fd_obj.name not in added:
            added.add(fd_obj.name)
            proto = FileDescriptorProto()
            fd_obj.CopyToProto(proto)
            fset.file.append(proto)

    # 处理输入（支持FileDescriptor或文件名）
    if hasattr(fd, 'name'):
        target_fd = fd
    else:
        target_fd = pool.FindFileByName(fd)
        if not target_fd:
            raise ValueError(f"FileDescriptor {fd} not found in pool!")
    
    collect_fd(target_fd)
    
    
    return fset.SerializeToString()

def write_record(path):
    
    new_desc_path = "/apollo/software/msg_converter/out/all.descriptor_set"
    fds = FileDescriptorSet()
    st = LoadFileDescriptorSetFromFile(new_desc_path, fds)
    if not st.ok():
        sys.exit(1)
    cyber.init()
    
    ctx = DynamicContext()
    out_bytes  = MakeDynamicContext(fds, ctx)
    
    out_fds_bytes = BuildMinimalFDSFromType(ctx.pool, "byd.msg.state_machine.TopState")
    print(f"out_fds_bytes:{out_fds_bytes}")
    
    
    fwriter = record.RecordWriter()
    fwriter.set_size_fileseg(0)
    fwriter.set_intervaltime_fileseg(0)
    if not fwriter.open(path):
        print('Failed to open record writer!')
        return

    # 其余示例通道（可选）
    # Error code
    msg4 = StatusPb()
    msg4.msg = "hello wuti"
    channel_name = f'/drivers/{msg4.DESCRIPTOR.full_name.replace(".", "_")}'
    # channel_name = f'/drivers/error_code'
    fd4 = msg4.DESCRIPTOR.file
    desc_bytes4 = build_file_descriptor_set(fd4)
    fwriter.write_channel(channel_name, msg4.DESCRIPTOR.full_name, desc_bytes4)
    fwriter.write_message(channel_name, msg4.SerializeToString(), 996)

    # Header
    msg5 = Header()
    msg5.frame_id = "6541"
    msg5.status.msg = "header-> statuspb"
    channel_name = f'/drivers/{msg5.DESCRIPTOR.full_name.replace(".", "_")}'
    # channel_name = f'/drivers/header'
    fd5 = msg5.DESCRIPTOR.file
    desc_bytes5 = build_file_descriptor_set(fd5)
    fwriter.write_channel(channel_name, msg5.DESCRIPTOR.full_name, desc_bytes5)
    fwriter.write_message(channel_name, msg5.SerializeToString(), 998)
    pool = descriptor_pool.Default()
    descriptor = pool.FindMessageTypeByName(msg5.DESCRIPTOR.full_name)
    print(f"header descriptor:{descriptor} ")
    
    msg6 = Point2D()
    msg6.y = 1.8
    fd6 = msg6.DESCRIPTOR.file
    channel_name = f'/drivers/{msg6.DESCRIPTOR.full_name.replace(".", "_")}'
    desc_bytes6 = build_file_descriptor_set(fd6)
    fwriter.write_channel(channel_name, msg6.DESCRIPTOR.full_name, desc_bytes6)
    fwriter.write_message(channel_name, msg6.SerializeToString(), 998)
        
    msg7 = Point3D()
    msg7.x = 3.5
    fd7 = msg7.DESCRIPTOR.file
    channel_name = f'/drivers/{msg7.DESCRIPTOR.full_name.replace(".", "_")}'
    desc_bytes7 = build_file_descriptor_set(fd7)
    fwriter.write_channel(channel_name, msg7.DESCRIPTOR.full_name, desc_bytes7)
    fwriter.write_message(channel_name, msg7.SerializeToString(), 998)
    


    # 现在写 TopState（其 FileDescriptorSet 也会包含依赖）
    msg6 = TopState()
    msg6.system_state = 0
    msg6.state = 2
    fd6 = msg6.DESCRIPTOR.file
    desc_bytes6 = build_file_descriptor_set(fd6)
    print("topstate type:", msg6.DESCRIPTOR.full_name)
    debug_print_descriptor_names(desc_bytes6)
    # print(f"desc_bytes6:{desc_bytes6}")
    fwriter.write_channel('/state_machine/top_state/state', msg6.DESCRIPTOR.full_name, desc_bytes6)
    fwriter.write_message('/state_machine/top_state/state', msg6.SerializeToString(), 994)

    msg8 = SensorParameters()
    msg8.vehicle_id = "dsawuyi321321"
    fd8 = msg8.DESCRIPTOR.file
    desc_bytes8 = build_file_descriptor_set(fd8)
    debug_print_descriptor_names(desc_bytes8)
    print("SensorParameters full name:", msg8.DESCRIPTOR.full_name)
    fwriter.write_channel('/calibration', msg8.DESCRIPTOR.full_name, desc_bytes8)
    fwriter.write_message('/calibration', msg8.SerializeToString(), 998)

    fwriter.close()
    print("Wrote record:", path)

if __name__ == '__main__':
    write_record('./test_writer_with_deps_first.record')
