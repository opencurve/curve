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
#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_ATTACHMENT_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_ATTACHMENT_H_

#include <braft/snapshot.h>
#include <string>
#include <vector>
#include <memory>

#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/datastore/datastore_file_helper.h"

namespace curve {
namespace chunkserver {

/**
 * 用于获取snapshot attachment files的接口，一般用于一些下载
 * 快照获取需要额外下载的文件list
 */
class SnapshotAttachment :
               public butil::RefCountedThreadSafe<SnapshotAttachment> {
 public:
    SnapshotAttachment() = default;
    virtual ~SnapshotAttachment() = default;

    /**
     * 获取snapshot attachment文件列表
     * @param files[out]: attachment文件列表
     * @param snapshotPath[in]: braft快照的路径
     */
    virtual void list_attach_files(std::vector<std::string> *files,
        const std::string& raftSnapshotPath) = 0;
};

// SnapshotAttachment接口的实现，用于raft加载快照时，获取chunk快照文件列表
class CurveSnapshotAttachment : public SnapshotAttachment {
 public:
    explicit CurveSnapshotAttachment(std::shared_ptr<LocalFileSystem> fs);
    virtual ~CurveSnapshotAttachment() = default;
    /**
     * 获取raft snapshot的attachment，这里就是获取chunk的快照文件列表
     * @param files[out]: data目录下的chunk快照文件列表
     * @param raftSnapshotPath: braft快照的路径
     * 返回的文件路径使用 绝对路径:相对路径 的格式,相对路径包含data目录
     */
    void list_attach_files(std::vector<std::string> *files,
                           const std::string& raftSnapshotPath) override;
 private:
    DatastoreFileHelper fileHelper_;
};

/*
* @brif 通过具体的某个raft的snapshot实例地址获取raft实例基础地址
* @param[in] specificSnapshotDir 某个具体snapshot的目录
        比如/data/chunkserver1/copysets/4294967812/raft_snapshot/snapshot_805455/
* @param[in] raftSnapshotRelativeDir 上层业务指的所有snapshot的相对基地址
        比如raft_snapshot
* @return 返回raft实例的绝对基地址，/data/chunkserver1/copysets/4294967812/
*/
inline std::string getCurveRaftBaseDir(std::string specificSnapshotDir,
    std::string raftSnapshotRelativeDir) {
    std::string::size_type m =
        specificSnapshotDir.find(raftSnapshotRelativeDir);
    if (m == std::string::npos) {
        return "";
    } else {
        return specificSnapshotDir.substr(0, m);
    }
}

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_ATTACHMENT_H_
