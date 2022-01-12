/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-09-09
 * @Author: majie1
 */

#ifndef CURVEFS_SRC_METASERVER_S3COMPACT_H_
#define CURVEFS_SRC_METASERVER_S3COMPACT_H_

#include <memory>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {
using curvefs::common::PartitionInfo;

// one S3Compact per partition
class S3Compact {
 public:
    explicit S3Compact(std::shared_ptr<InodeStorage> inodeStorage,
                       const PartitionInfo& partitionInfo)
        : inodeStorage_(inodeStorage), partitionInfo_(partitionInfo) {}

    std::shared_ptr<InodeStorage> GetMutableInodeStorage() {
        return inodeStorage_;
    }

    PartitionInfo GetPartition() const {
        return partitionInfo_;
    }

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    // we only need data that will not be changed after being created.
    const PartitionInfo partitionInfo_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3COMPACT_H_
