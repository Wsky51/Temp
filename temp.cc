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
