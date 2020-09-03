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
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/nameserver2/idgenerator/etcd_id_generator.h"
#include "src/common/string_util.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

namespace curve {
namespace mds {
bool EtcdIdGenerator::GenID(InodeID *id) {
    ::curve::common::WriteLockGuard guard(lock_);
    if (nextId_ > bundleEnd_ || nextId_ == initialize_) {
        if (!AllocateBundleIds(bundle_)) {
            return false;
        }
    }

    *id = nextId_++;
    return true;
}

bool EtcdIdGenerator::AllocateBundleIds(int requiredNum) {
    // get the maximum value that has been allocated
    std::string out = "";
    uint64_t alloc;
    int errCode = client_->Get(storeKey_, &out);
    // failed
    if (EtcdErrCode::EtcdOK != errCode &&
        EtcdErrCode::EtcdKeyNotExist != errCode) {
        LOG(ERROR) << "get store key: " << storeKey_
                   << " err, errCode: " << errCode;
        return false;
    } else if (EtcdErrCode::EtcdKeyNotExist == errCode) {
        // key not exist, indicates the first allocation
        alloc = initialize_;
    } else if (!NameSpaceStorageCodec::DecodeID(out, &alloc)) {
        // The value corresponding to the key exists, but the decode fails,
        // indicating that an internal err has occurred, alarm!
        LOG(ERROR) << "decode id: " << out << "err";
        return false;
    }

    uint64_t target = alloc + requiredNum;
    errCode = client_->CompareAndSwap(storeKey_, out,
        NameSpaceStorageCodec::EncodeID(target));
    if (EtcdErrCode::EtcdOK != errCode) {
        LOG(ERROR) << "do CAS {preV: " << out << ", target: " << target
                   << "err, errCode: " << errCode;
        return false;
    }

    // assign values ​​to next and end
    bundleEnd_ = target;
    nextId_ = alloc + 1;
    return true;
}
}  // namespace mds
}  // namespace curve
