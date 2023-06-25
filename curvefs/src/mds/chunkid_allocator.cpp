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
 * Created Date: 2021-08-27
 * Author: chengyi01
 */

#include <utility>

#include "curvefs/src/mds/chunkid_allocator.h"

namespace curvefs {
namespace mds {

int ChunkIdAllocatorImpl::GenChunkId(uint64_t idNum, uint64_t *chunkId) {
    int ret = 0;
    // will unloack at destructor
    ::curve::common::WriteLockGuard guard(nextIdRWlock_);

    if (nextId_ + idNum - 1> lastId_ || CHUNKIDINITIALIZE == nextId_) {
        // the chunkIds is exhausted || first to get chunkId
        uint64_t allocSize = nextId_ + idNum - lastId_ + bundleSize_;
        ret = AllocateBundleIds(allocSize);
    }

    if (ret >= 0) {
        // allocate id
        *chunkId = nextId_;
        nextId_ += idNum;
        VLOG(3) << "allocate chunkid:" << *chunkId;
    } else {
        // the chunkIds in the current bundle is exhausted,
        // but fail to get a new bunlde of chunIds.
        LOG(ERROR) << "allocate chunkid error, error code is: " << ret;
    }
    return ret;
}

void ChunkIdAllocatorImpl::Init(const std::shared_ptr<StorageClient> &client,
                                const std::string &StoreKey,
                                uint64_t bundleSize) {
    client_ = std::move(client);
    storeKey_ = StoreKey;
    bundleSize_ = bundleSize;
}

int ChunkIdAllocatorImpl::AllocateBundleIds(int bundleSize) {
    // get the maximum value that has been allocated
    std::string out;

    int statCode = client_->Get(storeKey_, &out);
    int ret = 0;
    uint64_t alloc = nextId_;
    if (EtcdErrCode::EtcdOK != statCode &&
        EtcdErrCode::EtcdKeyNotExist != statCode) {
        // -1: other error
        LOG(ERROR) << "get store key: " << storeKey_
                   << " err, errCode: " << statCode;
        ret = UNKNOWN_ERROR;
    } else if (EtcdErrCode::EtcdKeyNotExist == statCode) {
        // key not exist
        LOG(WARNING) << "key " << storeKey_ << " not found in etcd";
    } else if (!DecodeID(out, &alloc)) {
        // -2: value decode fails
        // The value corresponding to the key exists, but the decode fails,
        // indicating that an internal err has occurred
        LOG(ERROR) << "decode id: " << out << " err";
        ret = DECODE_ERROR;
    } else {
        // key exist and can be decode
        lastId_ = alloc;
    }

    if (ret < 0) {
        // get value Error
        return ret;
    }

    uint64_t target = lastId_ + bundleSize;
    statCode = client_->CompareAndSwap(storeKey_, out, EncodeID(target));
    if (EtcdErrCode::EtcdOK != statCode) {
        // -3: CAS error
        LOG(ERROR) << "do CAS {preV: " << alloc << ", target: " << target
                   << " err, errCode: " << statCode;
        ret = CAS_ERROR;
    } else {
        // allocate ok
        nextId_ = alloc + 1;
        lastId_ = target;
        LOG(INFO) << "allocate chunkId form " << out << " to " << target;
    }

    return ret;
}

bool ChunkIdAllocatorImpl::DecodeID(const std::string &value, uint64_t *out) {
    return ::curve::common::StringToUll(value, out);
}

}  // namespace mds
}  // namespace curvefs
