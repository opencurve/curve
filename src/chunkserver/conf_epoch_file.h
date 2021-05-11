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
 * Created Date: 18-12-20
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_
#define SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_

#include <string>
#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/fs/fs_common.h"
#include "include/chunkserver/chunkserver_common.h"
#include "proto/copyset.pb.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

/**
 * Tool classes for epoch serialisation and deserialisation
 * TODO(wudemiao): replace it with json encoding afterwards
 */
class ConfEpochFile {
 public:
    explicit ConfEpochFile(std::shared_ptr<LocalFileSystem> fs)
        : fs_(fs) {}

    /**
     * Load the configuration epoch in the snapshot file
     * @param path:File path
     * @param logicPoolID:Logic pool id
     * @param copysetID:Copyset id
     * @param epoch:Configure the epoch, the params, and return the epoch value
     * @return Return 0 for success, -1 for failure
     */
    int Load(const std::string &path,
             LogicPoolID *logicPoolID,
             CopysetID *copysetID,
             uint64_t *epoch);

    /**
     * The configuration epoch information is saved to the snapshot file in
     * the following serialised format, with head indicating length and
     * using binary. Everything else is in text format, so that it can be viewed
     * directly if necessary, and sync ensures that the data falls on disk
     * |              head           |          configuration epoch information|
     * | 8 bytes size_t   | uint32_t |              Variable text              |
     * |     length       |   crc32  | logic pool id | copyset id | epoch |
     * The persistence above is separated by ':'
     * @param path:File path
     * @param logicPoolID:Logic pool id
     * @param copysetID:Copyset id
     * @param epoch:Configure the epoch,
     * @return Return 0 for success, -1 for failure
     */
    int Save(const std::string &path,
             const LogicPoolID logicPoolID,
             const CopysetID copysetID,
             const uint64_t epoch);

 private:
    static uint32_t ConfEpochCrc(const ConfEpoch &confEpoch);

    std::shared_ptr<LocalFileSystem> fs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONF_EPOCH_FILE_H_
