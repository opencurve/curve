/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Monday Feb 28 14:30:05 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_BLOCK_GROUP_STORAGE_H_
#define CURVEFS_SRC_MDS_SPACE_BLOCK_GROUP_STORAGE_H_

#include <memory>
#include <vector>

#include "curvefs/proto/space.pb.h"
#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {
namespace space {

class BlockGroupStorage {
 public:
    virtual ~BlockGroupStorage() = default;

    virtual SpaceErrCode PutBlockGroup(uint32_t fsId,
                                       uint64_t offset,
                                       const BlockGroup& blockGroup) = 0;

    virtual SpaceErrCode RemoveBlockGroup(uint32_t fsId, uint64_t offset) = 0;

    virtual SpaceErrCode ListBlockGroups(
        uint32_t fsId,
        std::vector<BlockGroup>* blockGroups) = 0;
};

class BlockGroupStorageImpl final : public BlockGroupStorage {
 public:
    explicit BlockGroupStorageImpl(
        const std::shared_ptr<curve::kvstorage::KVStorageClient>& store)
        : store_(store) {}

    BlockGroupStorageImpl(const BlockGroupStorageImpl&) = delete;
    BlockGroupStorageImpl& operator=(const BlockGroupStorageImpl&) = delete;

    /**
     * @brief Insert one block group into storage
     * @return if success return SpaceOk, otherwise return error code
     */
    SpaceErrCode PutBlockGroup(uint32_t fsId,
                               uint64_t offset,
                               const BlockGroup& blockGroup) override;

    /**
     * @brief Remove a block group from storage
     * @return if success return SpaceOk, otherwise return error code
     */
    SpaceErrCode RemoveBlockGroup(uint32_t fsId, uint64_t offset) override;

    /**
     * @brief List all block groups from storage that belong to a single fs
     */
    SpaceErrCode ListBlockGroups(uint32_t fsId,
                                 std::vector<BlockGroup>* blockGroups) override;

 private:
    std::shared_ptr<curve::kvstorage::KVStorageClient> store_;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_BLOCK_GROUP_STORAGE_H_
