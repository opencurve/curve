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
 * Project: curve
 * Created Date: 18-12-20
 * Author: wudemiao
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_

#include <memory>
#include <string>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/src/metaserver/common/types.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

// TODO(wuhanqing): this file is identical to src/chunkserver/conf_epoch_file.h

class ConfEpochFile {
 public:
    explicit ConfEpochFile(curve::fs::LocalFileSystem* fs) : fs_(fs) {}

    /**
     * Load the configuration version in the snapshot file
     * @param path: File path
     * @param logicPoolID: Logical Pool ID
     * @param copysetID: Copy group ID
     * @param epoch: Configuration version, output parameters, return the read
     * epoch value
     * @return 0, successful; - 1 failed
     */
    int Load(const std::string& path, PoolId* poolId, CopysetId* copysetId,
             uint64_t* epoch);

    /**
     * Serialize configuration version information and save it to a snapshot
     *file. The format is as follows: The 'head' indicates the length and is in
     *binary format. The rest is in text format for easy viewing when necessary.
     *'sync' ensures data persistence. |            head |Configuration version
     *information| | 8 bytes size_t   | uint32_t |       Variable length text |
     *|     length       | crc32  | logic pool id | copyset id | epoch|
     * The persistence above is separated by ':'
     * @param path: File path
     * @param logicPoolID: Logical Pool ID
     * @param copysetID: Copy group ID
     * @param epoch: Configuration version
     * @return 0 succeeded; - 1 failed
     */
    int Save(const std::string& path, const PoolId poolId,
             const CopysetId copysetId, const uint64_t epoch);

 private:
    static uint32_t ConfEpochCrc(const ConfEpoch& confEpoch);

    curve::fs::LocalFileSystem* fs_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_CONF_EPOCH_FILE_H_
