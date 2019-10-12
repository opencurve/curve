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
    RaftSnapshotAttachment(const std::string& dataDir,
                           std::shared_ptr<LocalFileSystem> fs);
    void list_attach_files(std::vector<std::string> *files) override;
 private:
    // copyset的数据文件所在目录
    std::string dataDir_;
    DatastoreFileHelper fileHelper_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_ATTACHMENT_H_
