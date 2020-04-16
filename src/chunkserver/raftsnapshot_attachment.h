/*
 * Project: curve
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */
#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_ATTACHMENT_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_ATTACHMENT_H_

#include <braft/snapshot.h>
#include <string>
#include <vector>
#include <memory>

#include "src/chunkserver/datastore/datastore_file_helper.h"

namespace curve {
namespace chunkserver {

// SnapshotAttachment接口的实现，用于raft加载快照时，获取chunk快照文件列表
class RaftSnapshotAttachment : public braft::SnapshotAttachment {
 public:
    explicit RaftSnapshotAttachment(std::shared_ptr<LocalFileSystem> fs);
    /**
     * 获取raft snapshot的attachment，这里就是获取chunk的快照文件列表
     * @param files[out]: data目录下的chunk快照文件列表
     * @param raftBaseDir: 要获取的copyset的绝对路径
     * 返回的文件路径使用 绝对路径:相对路径 的格式,相对路径包含data目录
     */
    void list_attach_files(std::vector<std::string> *files,
                           const std::string& raftBaseDir) override;
 private:
    DatastoreFileHelper fileHelper_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_ATTACHMENT_H_
