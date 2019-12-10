/*
 * Project: curve
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include "src/chunkserver/raftsnapshot_attachment.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {

RaftSnapshotAttachment::RaftSnapshotAttachment(
    std::shared_ptr<LocalFileSystem> fs)
    : fileHelper_(fs) {}

void RaftSnapshotAttachment::list_attach_files(
    std::vector<std::string> *files, const std::string& raftBaseDir) {
    std::string dataDir;
    if (raftBaseDir[raftBaseDir.length()-1] != '/') {
        dataDir = raftBaseDir + "/" + RAFT_DATA_DIR;
    } else {
        dataDir = raftBaseDir + RAFT_DATA_DIR;
    }

    std::vector<std::string> snapFiles;
    int rc = fileHelper_.ListFiles(dataDir, nullptr, &snapFiles);
    // list出错一般认为就是磁盘出现问题了，这种情况直接让进程挂掉
    // Attention: 这里还需要更仔细考虑
    CHECK(rc == 0) << "List dir failed.";

    files->clear();
    // 文件路径格式与snapshot_meta中的格式要相同
    for (const auto& snapFile : snapFiles) {
        std::string filePath;
        // 添加绝对路径
        filePath.append(dataDir);
        filePath.append("/").append(snapFile);
        // 添加分隔符
        filePath.append(":");
        // 添加相对路径
        filePath.append(RAFT_DATA_DIR);
        filePath.append("/").append(snapFile);
        files->emplace_back(filePath);
    }
}


}  // namespace chunkserver
}  // namespace curve
