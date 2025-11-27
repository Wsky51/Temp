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




