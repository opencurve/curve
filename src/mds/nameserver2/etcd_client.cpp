/*
 * Project: curve
 * Created Date: Saturday March 9th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <cassert>
#include "src/mds/nameserver2/etcd_client.h"
#include "src/common/string_util.h"

namespace curve {
namespace mds {
int EtcdClientImp::Init(EtcdConf conf, int timeout, int retryTimes) {
    this->timeout_ = timeout;
    this->retryTimes_ = retryTimes;
    return NewEtcdClientV3(conf);
}

void EtcdClientImp::CloseClient() {
    EtcdCloseClient();
}

int EtcdClientImp::Put(const std::string &key, const std::string &value) {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        errCode = EtcdClientPut(timeout_, const_cast<char*>(key.c_str()),
            const_cast<char*>(value.c_str()), key.size(), value.size());
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= retryTimes_);

    return errCode;
}

int EtcdClientImp::Get(const std::string &key, std::string *out) {
    assert(out != nullptr);
    out->clear();

    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        EtcdClientGet_return res = EtcdClientGet(
            timeout_, const_cast<char*>(key.c_str()), key.size());
        errCode = res.r0;
        needRetry = NeedRetry(errCode);
        if (res.r0 == EtcdErrCode::OK) {
            *out = std::string(res.r1, res.r1 + res.r2);
            free(res.r1);
        } else if (res.r0 == EtcdErrCode::KeyNotExist) {
            LOG(INFO) << "file not exist";
        } else {
            LOG(ERROR) << "get file err: " << res.r0;
        }
    } while (needRetry && ++retry <= retryTimes_);

    return errCode;
}

int EtcdClientImp::List(const std::string &startKey, const std::string &endKey,
    std::vector<std::string> *out) {
    assert(out != nullptr);
    out->clear();

    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        EtcdClientList_return res = EtcdClientList(
            timeout_, const_cast<char*>(startKey.c_str()),
            const_cast<char*>(endKey.c_str()), startKey.size(), endKey.size());
        errCode = res.r0;
        needRetry = NeedRetry(errCode);
        if (res.r0 != EtcdErrCode::OK) {
            LOG(ERROR) << "list file of [start:" << startKey
                    << ", end:" << endKey << "] err:" << res.r0;
        } else {
            for (int i = 0; i < res.r2; i++) {
                EtcdClientGetMultiObject_return objRes =
                    EtcdClientGetMultiObject(res.r1, i);
                if (objRes.r0 != EtcdErrCode::OK) {
                    LOG(ERROR) << "get object:" << res.r1 << " index: " << i
                            << "err: " << objRes.r0;
                    EtcdClientRemoveObject(res.r1);
                    return objRes.r0;
                }

                out->emplace_back(
                    std::string(objRes.r1, objRes.r1 + objRes.r2));
                free(objRes.r1);
            }
            EtcdClientRemoveObject(res.r1);
        }
    } while (needRetry && ++retry <= retryTimes_);

    return errCode;
}

int EtcdClientImp::Delete(const std::string &key) {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        errCode = EtcdClientDelete(
            timeout_, const_cast<char*>(key.c_str()), key.size());
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= retryTimes_);
    return errCode;
}

int EtcdClientImp::TxnN(const std::vector<Operation> &ops) {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        if (ops.size() == 2) {
            errCode = EtcdClientTxn2(timeout_, ops[0], ops[1]);
        } else if (ops.size() == 3) {
            errCode = EtcdClientTxn3(timeout_, ops[0], ops[1], ops[2]);
        } else {
            LOG(ERROR) << "do not support Txn " << ops.size();
            return EtcdErrCode::InvalidArgument;
        }
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= retryTimes_);
    return errCode;
}

int EtcdClientImp::CompareAndSwap(const std::string &key,
    const std::string &preV, const std::string &target) {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        errCode = EtcdClientCompareAndSwap(
            timeout_, const_cast<char*>(key.c_str()),
            const_cast<char*>(preV.c_str()), const_cast<char*>(target.c_str()),
            key.size(), preV.size(), target.size());
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= retryTimes_);
    return errCode;
}

int EtcdClientImp::CampaignLeader(
    const std::string &pfx, const std::string &leaderName,
    uint32_t sessionInterSec, uint32_t electionTimeoutMs, uint64_t *leaderOid) {
    LOG(INFO) << "campaign leader, pfx{" << pfx
              << "}, leaderName{" << leaderName << "}";
    EtcdElectionCampaign_return res = EtcdElectionCampaign(
        const_cast<char*>(pfx.c_str()), pfx.size(),
        const_cast<char*>(leaderName.c_str()), leaderName.size(),
        sessionInterSec, electionTimeoutMs);
    if (res.r0 == EtcdErrCode::CampaignLeaderSuccess) {
        *leaderOid = res.r1;
    }
    return res.r0;
}

int EtcdClientImp::LeaderObserve(
    uint64_t leaderOid, uint64_t timeoutMs, const std::string &leaderName) {
    return EtcdLeaderObserve(leaderOid, timeoutMs,
        const_cast<char*>(leaderName.c_str()), leaderName.size());
}

int EtcdClientImp::LeaderResign(uint64_t leaderOid, uint64_t timeoutMs) {
    return EtcdLeaderResign(leaderOid, timeoutMs);
}

void EtcdClientImp::SetTimeout(int timeout) {
    this->timeout_ = timeout;
}

bool EtcdClientImp::NeedRetry(int errCode) {
    switch (errCode) {
        case EtcdErrCode::OK:
        case EtcdErrCode::Canceled:
        case EtcdErrCode::Unknown:
        case EtcdErrCode::InvalidArgument:
        case EtcdErrCode::NotFound:
        case EtcdErrCode::AlreadyExists:
        case EtcdErrCode::PermissionDenied:
        case EtcdErrCode::OutOfRange:
        case EtcdErrCode::Unimplemented:
        case EtcdErrCode::Internal:
        case EtcdErrCode::DataLoss:
        case EtcdErrCode::Unauthenticated:
        case EtcdErrCode::TxnUnkownOp:
        case EtcdErrCode::KeyNotExist:
            return false;

        case EtcdErrCode::DeadlineExceeded:
        case EtcdErrCode::ResourceExhausted:
        case EtcdErrCode::FailedPrecondition:
        case EtcdErrCode::Aborted:
        case EtcdErrCode::Unavailable:
            return true;
    }
    return false;
}

}  // namespace mds
}  // namespace curve

