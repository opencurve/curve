/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
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
