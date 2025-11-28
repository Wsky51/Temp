  Status LoadFileDescriptorSetFromFile(const std::string& path,
                                     google::protobuf::FileDescriptorSet* out) {
  if (out == nullptr) {
    std::cerr << "[ERROR] Descriptor loader: out=nullptr\n";
    return Status::InvalidArgument("out is null");
  }
  std::ifstream ifs(path, std::ios::binary);
  if (!ifs) {
    std::cerr << "[ERROR] Cannot open desc file: " << path << "\n";
    return Status::IoOrDescriptorFatal("open desc failed: " + path);
  }
  if (!out->ParseFromIstream(&ifs)) {
    std::cerr << "[ERROR] Parse FileDescriptorSet failed: " << path << "\n";
    return Status::IoOrDescriptorFatal("parse desc failed: " + path);
  }
  std::cout << "[INFO] Loaded FileDescriptorSet: " << path << "\n";
  // std::cout << "for debug FileDescriptorSet:" << out->DebugString()
  //           << std::endl;
  return Status::Ok();
}


std::string new_desc_path = "out/all.descriptor_set";
  // Load new descriptors
  google::protobuf::FileDescriptorSet new_fds;
  {
    auto st = LoadFileDescriptorSetFromFile(new_desc_path, &new_fds);
    if (!st.ok()) return st;
  }





Status MakeDynamicContext(const google::protobuf::FileDescriptorSet& fds,
                          DynamicContext* ctx) {
  if (ctx == nullptr) return Status::InvalidArgument("ctx is null");
  auto db = std::make_unique<google::protobuf::SimpleDescriptorDatabase>();
  for (const auto& fd : fds.file()) {
    if (!db->Add(fd)) {
      std::cerr << "[ERROR] DescriptorDatabase Add failed for: " << fd.name()
                << "\n";
      return Status::IoOrDescriptorFatal("db add failed: " + fd.name());
    }
  }
  auto pool = std::make_unique<DBBackedPool>(std::move(db));
  auto factory =
      std::make_unique<google::protobuf::DynamicMessageFactory>(pool.get());
  ctx->pool = std::move(pool);
  ctx->factory = std::move(factory);
  std::cout << "[INFO] DynamicContext created with " << fds.file_size()
            << " files\n";
  return Status::Ok();
}




#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

using google::protobuf::Descriptor;
using google::protobuf::DescriptorPool;
using google::protobuf::FileDescriptor;
using google::protobuf::FileDescriptorSet;

inline bool BuildMinimalFDSFromType(
    const google::protobuf::DescriptorPool& pool,
    const std::string& type_full_name, std::string* out_fds_bytes) {
  if (out_fds_bytes == nullptr) {
    return false;
  }

  // 1. 根据类型全名获取Descriptor
  const Descriptor* descriptor = pool.FindMessageTypeByName(type_full_name);
  if (descriptor == nullptr) {
    return false;
  }

  // 2. 获取对应的FileDescriptor
  const FileDescriptor* file_desc = descriptor->file();
  if (file_desc == nullptr) {
    return false;
  }

  FileDescriptorSet file_descriptor_set;
  std::set<std::string> processed_files;

  // 3. 递归收集所有依赖的文件描述符
  std::function<void(const FileDescriptor*)> collect_dependencies =
      [&](const FileDescriptor* current_file) {
        if (current_file == nullptr) return;

        std::string file_name = current_file->name();
        if (processed_files.count(file_name) > 0) return;

        processed_files.insert(file_name);

        // 先递归处理所有依赖项
        for (int i = 0; i < current_file->dependency_count(); ++i) {
          collect_dependencies(current_file->dependency(i));
        }

        // 将当前文件描述符添加到FileDescriptorSet中
        google::protobuf::FileDescriptorProto* file_proto =
            file_descriptor_set.add_file();
        current_file->CopyTo(file_proto);

        // 确保包含完整的依赖信息
        file_proto->clear_dependency();
        for (int i = 0; i < current_file->dependency_count(); ++i) {
          file_proto->add_dependency(current_file->dependency(i)->name());
        }
      };

  // 4. 从目标文件开始收集
  collect_dependencies(file_desc);

  // 5. 序列化为字节字符串
  return file_descriptor_set.SerializeToString(out_fds_bytes);
}



#!/usr/bin/env python3
# 用依赖优先（post-order）方式构造 FileDescriptorSet，再写入 record。
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from google.protobuf import descriptor_pool

from cyber.python.cyber_py3 import record
from cyber.proto.simple_pb2 import SimpleMessage
from cyber.proto.unit_test_pb2 import Chatter
from modules.msg.state_machine_msgs.top_state_pb2 import TopState
from modules.msg.basic_msgs.error_code_pb2 import StatusPb
from modules.msg.basic_msgs.header_pb2 import Header

def build_file_descriptor_set(fd):
    """
    构建包含 fd 及其所有传递依赖的 FileDescriptorSet，
    并保证 FileDescriptorProto 的顺序是：依赖先加入（post-order）。
    接受 FileDescriptor 或文件名字符串，返回序列化后的 bytes。
    """
    pool = descriptor_pool.Default()
    fset = FileDescriptorSet()
    seen = set()
    added = set()
    # 使用名字作为唯一 key
    def collect_names(name):
        """收集所有依赖的文件名（递归），返回按依赖优先的列表（不重复）。"""
        if name in seen:
            return
        seen.add(name)
        try:
            fd_obj = pool.FindFileByName(name)
        except Exception as e:
            print(f"Warning: cannot find file descriptor by name '{name}': {e}")
            return
        # 先递归依赖
        proto_tmp = FileDescriptorProto()
        fd_obj.CopyToProto(proto_tmp)
        for dep in proto_tmp.dependency:
            collect_names(dep)
        # 然后把当前文件名加入结果（post-order）
        if proto_tmp.name not in added:
            added.add(proto_tmp.name)
            fset.file.append(proto_tmp)

    if hasattr(fd, 'name'):
        start_name = fd.name
    else:
        start_name = fd
    collect_names(start_name)
    return fset.SerializeToString()

def debug_print_descriptor_names(desc_bytes):
    from google.protobuf.descriptor_pb2 import FileDescriptorSet
    f = FileDescriptorSet()
    f.ParseFromString(desc_bytes)
    print("DescriptorSet contains files in this order:")
    for pf in f.file:
        print(" -", pf.name)

def write_record(path):
    fwriter = record.RecordWriter()
    fwriter.set_size_fileseg(0)
    fwriter.set_intervaltime_fileseg(0)
    if not fwriter.open(path):
        print('Failed to open record writer!')
        return

    # TopState (重点)
    msg6 = TopState()
    msg6.system_state = 0
    msg6.state = 2
    fd6 = msg6.DESCRIPTOR.file
    desc_bytes6 = build_file_descriptor_set(fd6)
    print("topstate type:", msg6.DESCRIPTOR.full_name)
    debug_print_descriptor_names(desc_bytes6)
    fwriter.write_channel('/state_machine/top_state/state', msg6.DESCRIPTOR.full_name, desc_bytes6)
    fwriter.write_message('/state_machine/top_state/state', msg6.SerializeToString(), 994)

    # 其余示例通道（可选）
    # Error code
    msg4 = StatusPb()
    msg4.msg = "hello wuti"
    fd4 = msg4.DESCRIPTOR.file
    desc_bytes4 = build_file_descriptor_set(fd4)
    fwriter.write_channel('/driver/error_code', msg4.DESCRIPTOR.full_name, desc_bytes4)
    fwriter.write_message('/driver/error_code', msg4.SerializeToString(), 996)

    # Header
    msg5 = Header()
    msg5.frame_id = "6541"
    msg5.status.msg = "header-> statuspb"
    fd5 = msg5.DESCRIPTOR.file
    desc_bytes5 = build_file_descriptor_set(fd5)
    fwriter.write_channel('/driver/header', msg5.DESCRIPTOR.full_name, desc_bytes5)
    fwriter.write_message('/driver/header', msg5.SerializeToString(), 998)

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
  
    msg8 = SensorParameters()
    msg8.vehicle_id = "dsawuyi321321"
    fd8 = msg8.DESCRIPTOR.file
    desc_bytes8 = build_file_descriptor_set(fd8)
    debug_print_descriptor_names(desc_bytes8)
    fwriter.write_channel('/calibration', msg8.DESCRIPTOR.full_name, desc_bytes8)
    fwriter.write_message('/calibration', msg8.SerializeToString(), 998)

    fwriter.close()
    print("Wrote record:", path)

if __name__ == '__main__':
    write_record('./test_writer_ordered.record')
