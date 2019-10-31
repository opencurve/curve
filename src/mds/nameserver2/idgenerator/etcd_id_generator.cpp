/*
 * Project: curve
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
    // 获取已经allocate的最大值
    std::string out = "";
    uint64_t alloc;
    int errCode = client_->Get(storeKey_, &out);
    // 获取失败
    if (EtcdErrCode::OK != errCode && EtcdErrCode::KeyNotExist != errCode) {
        LOG(ERROR) << "get store key: " << storeKey_
                   << " err, errCode: " << errCode;
        return false;
    } else if (EtcdErrCode::KeyNotExist == errCode) {
        // key尚未存在，说明是初次分配
        alloc = initialize_;
    } else if (!NameSpaceStorageCodec::DecodeID(out, &alloc)) {
        // key对应的value存在，但是decode失败，说明出现了internal err, 报警
        LOG(ERROR) << "decode id: " << out << "err";
        return false;
    }

    uint64_t target = alloc + requiredNum;
    errCode = client_->CompareAndSwap(storeKey_, out,
        NameSpaceStorageCodec::EncodeID(target));
    if (EtcdErrCode::OK != errCode) {
        LOG(ERROR) << "do CAS {preV: " << out << ", target: " << target
                   << "err, errCode: " << errCode;
        return false;
    }

    // 给next和end赋值
    bundleEnd_ = target;
    nextId_ = alloc + 1;
    return true;
}
}  // namespace mds
}  // namespace curve
