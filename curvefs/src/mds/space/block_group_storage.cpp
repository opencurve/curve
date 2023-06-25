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
 * Date: Monday Feb 28 14:52:29 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/block_group_storage.h"

#include <glog/logging.h>

#include <string>
#include <utility>

#include "curvefs/src/mds/codec/codec.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curve::kvstorage::StorageClient;
using ::curvefs::mds::codec::DecodeProtobufMessage;
using ::curvefs::mds::codec::EncodeBlockGroupKey;
using ::curvefs::mds::codec::EncodeProtobufMessage;

namespace {

SpaceErrCode StoreErrCodeToSpaceErrCode(int code) {
    switch (code) {
        case EtcdErrCode::EtcdOK:
            return SpaceOk;

        case EtcdErrCode::EtcdKeyNotExist:
            return SpaceErrNotFound;

        default:
            return SpaceErrStorage;
    }
}

}  // namespace

SpaceErrCode BlockGroupStorageImpl::PutBlockGroup(
    uint32_t fsId,
    uint64_t offset,
    const BlockGroup& blockGroup) {
    std::string key = EncodeBlockGroupKey(fsId, offset);
    std::string value;

    if (!blockGroup.IsInitialized()) {
        LOG(WARNING) << "Block group is not initialized, fsId: " << fsId
                     << ", offset: " << offset;
        return SpaceErrEncode;
    }

    if (!EncodeProtobufMessage(blockGroup, &value)) {
        LOG(WARNING) << "Encode block group failed, fsId: " << fsId
                     << ", block group offset: " << offset;
        return SpaceErrEncode;
    }

    int err = store_->Put(key, value);
    if (err != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put block group failed, fsId: " << fsId
                   << ", block group offset: " << offset << ", err: " << err;
    }

    return StoreErrCodeToSpaceErrCode(err);
}

SpaceErrCode BlockGroupStorageImpl::RemoveBlockGroup(uint32_t fsId,
                                                     uint64_t offset) {
    std::string key = EncodeBlockGroupKey(fsId, offset);

    int err = store_->Delete(key);
    if (err != EtcdErrCode::EtcdOK) {
        LOG(WARNING) << "Remove block group failed, fsId: " << fsId
                     << ", block group offset: " << offset << ", err: " << err;
    }

    return StoreErrCodeToSpaceErrCode(err);
}

SpaceErrCode BlockGroupStorageImpl::ListBlockGroups(
    uint32_t fsId,
    std::vector<BlockGroup>* blockGroups) {
    std::string startKey = EncodeBlockGroupKey(fsId, 0);
    std::string endKey = EncodeBlockGroupKey(fsId + 1, 0);

    std::vector<std::string> raw;

    int err = store_->List(startKey, endKey, &raw);
    if (err != EtcdErrCode::EtcdOK) {
        LOG(WARNING) << "List block groups failed, fsId: " << fsId
                     << ", err: " << err;
        return StoreErrCodeToSpaceErrCode(err);
    }

    for (size_t i = 0; i < raw.size(); ++i) {
        BlockGroup group;
        bool ok = DecodeProtobufMessage(raw[i], &group);
        if (!ok) {
            blockGroups->clear();
            LOG(WARNING) << "Decode block group failed";
            return SpaceErrDecode;
        }

        blockGroups->push_back(std::move(group));
    }

    return SpaceOk;
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
