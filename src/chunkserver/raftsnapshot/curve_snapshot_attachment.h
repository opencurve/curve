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

#include <memory>
#include <string>
#include <vector>

#include "src/chunkserver/datastore/datastore_file_helper.h"
#include "src/chunkserver/raftsnapshot/define.h"

namespace curve {
namespace chunkserver {

/**
 * The interface used to obtain snapshot attachment files, usually used for some
 * downloads List of files that require additional downloads for snapshot
 * acquisition
 */
class SnapshotAttachment
    : public butil::RefCountedThreadSafe<SnapshotAttachment> {
 public:
    SnapshotAttachment() = default;
    virtual ~SnapshotAttachment() = default;

    /**
     * Obtain a list of snapshot attachment files
     * @param files[out]: attachment file list
     * @param snapshotPath[in]: Path to the brace snapshot
     */
    virtual void list_attach_files(std::vector<std::string>* files,
                                   const std::string& raftSnapshotPath) = 0;
};

// Implementation of the SnapshotAttachment interface, used to obtain a list of
// chunk snapshot files when loading snapshots in the raft
class CurveSnapshotAttachment : public SnapshotAttachment {
 public:
    explicit CurveSnapshotAttachment(std::shared_ptr<LocalFileSystem> fs);
    virtual ~CurveSnapshotAttachment() = default;
    /**
     *Obtain the attachment of the raft snapshot, which is the list of snapshot
     *files for the chunk
     * @param files[out]: List of chunk snapshot files in the data directory
     * @param raftSnapshotPath: Path to the brace snapshot
     * The returned file path uses an absolute path: in the format of a relative
     *path, which includes the data directory
     */
    void list_attach_files(std::vector<std::string>* files,
                           const std::string& raftSnapshotPath) override;

 private:
    DatastoreFileHelper fileHelper_;
};

/*
* @brif obtains the base address of a raft instance through the snapshot
instance address of a specific raft
* @param[in] specificSnapshotDir The directory of a specific snapshot
        For
example,/data/chunkserver1/copysets/4294967812/raft_snapshot/snapshot_805455/
* @param[in] raftSnapshotRelativeDir The relative base addresses of all
snapshots referred to by the upper level business For example, raft_ Snapshot
* @return returns the absolute base address of the raft
instance,/data/chunkserver1/copysets/4294967812/
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
