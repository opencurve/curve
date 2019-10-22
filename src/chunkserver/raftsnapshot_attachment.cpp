/*
 * Project: curve
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include "src/chunkserver/raftsnapshot_attachment.h"

namespace curve {
namespace chunkserver {

RaftSnapshotAttachment::RaftSnapshotAttachment(
    const std::string& dataDir, std::shared_ptr<LocalFileSystem> fs)
    : dataDir_(dataDir)
    , fileHelper_(fs) {}

void RaftSnapshotAttachment::list_attach_files(
    std::vector<std::string> *files) {
    int rc = fileHelper_.ListFiles(dataDir_, nullptr, files);
    // list出错一般认为就是磁盘出现问题了，这种情况直接让进程挂掉
    // Attention: 这里还需要更仔细考虑
    CHECK(rc == 0) << "List dir failed.";
}


}  // namespace chunkserver
}  // namespace curve
