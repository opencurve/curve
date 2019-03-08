/*
 * Project: curve
 * Created Date: Saturday March 9th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <cassert>
#include "src/mds/nameserver2/etcd_client.h"

namespace curve {
namespace mds {
int EtcdClientImp::Init(EtcdConf conf, int timeout) {
    this->timeout_ = timeout;
    return NewEtcdClientV3(conf);
}

int EtcdClientImp::Put(std::string key, std::string value) {
    return EtcdClientPut(
            timeout_,
            const_cast<char*>(key.c_str()), const_cast<char*>(value.c_str()),
            key.size(), value.size());
}

int EtcdClientImp::Get(std::string key, std::string *out) {
    assert(out != nullptr);
    out->clear();

    EtcdClientGet_return res = EtcdClientGet(
        timeout_, const_cast<char*>(key.c_str()), key.size());
    if (res.r0 == EtcdErrCode::StatusOK) {
        *out = std::string(res.r1, res.r1 + res.r2);
        free(res.r1);
    } else {
        LOG(ERROR) << "get file[" << key << "] err: " << res.r0;
    }

    return res.r0;
}

int EtcdClientImp::List(std::string startKey, std::string endKey,
    std::vector<std::string> *out) {
    assert(out != nullptr);
    out->clear();
    EtcdClientList_return res = EtcdClientList(
        timeout_, const_cast<char*>(startKey.c_str()),
        const_cast<char*>(endKey.c_str()), startKey.size(), endKey.size());

    if (res.r0 != EtcdErrCode::StatusOK) {
        LOG(ERROR) << "list file of [start:" << startKey
                   << ", end:" << endKey << "] err:" << res.r0;
        return res.r0;
    }

    for (int i = 0; i < res.r2; i++) {
        EtcdClientGetMultiObject_return objRes =
            EtcdClientGetMultiObject(res.r1, i);
        if (objRes.r0 != EtcdErrCode::StatusOK) {
            LOG(ERROR) << "get object:" << res.r1 << " index: " << i
                       << "err:" << objRes.r0;
            EtcdClientRemoveObject(res.r1);
            return objRes.r0;
        }

        out->emplace_back(std::string(objRes.r1, objRes.r1 + objRes.r2));
        free(objRes.r1);
    }
    EtcdClientRemoveObject(res.r1);
    return res.r0;
}

int EtcdClientImp::Delete(std::string key) {
    return EtcdClientDelete(
        timeout_, const_cast<char*>(key.c_str()), key.size());
}

int EtcdClientImp::Txn2(Operation op1, Operation op2) {
    return EtcdClientTxn2(timeout_, op1, op2);
}

void EtcdClientImp::SetTimeout(int timeout) {
    this->timeout_ = timeout;
}
}  // namespace mds
}  // namespace curve

