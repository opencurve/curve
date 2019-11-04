/*
 * Project: curve
 * Created Date: 18-9-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CLIENT_CHUNK_CLOSURE_H_
#define SRC_CLIENT_CHUNK_CLOSURE_H_

#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <brpc/errno.pb.h>
#include <unordered_map>  // NOLINT
#include <unordered_set>  // NOLINT

#include "proto/chunk.pb.h"
#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/client_metric.h"
#include "src/client/request_closure.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoResponse;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;
using curve::common::SpinLock;

class RequestSenderManager;
class MetaCache;
class CopysetClient;

class UnstableChunkServerHelper {
 public:
    static void IncreTimeout(ChunkServerID csId) {
        timeoutLock_.Lock();
        ++timeoutTimes_[csId];
        timeoutLock_.UnLock();
    }

    static bool IsTimeoutExceed(ChunkServerID csId) {
        timeoutLock_.Lock();
        auto ret = timeoutTimes_[csId] > maxStableChunkServerTimeoutTimes_;
        timeoutLock_.UnLock();
        return ret;
    }

    static void ClearTimeout(ChunkServerID csId) {
        timeoutLock_.Lock();
        timeoutTimes_[csId] = 0;
        timeoutLock_.UnLock();
    }

    static void SetMaxStableChunkServerTimeoutTimes(
        uint64_t maxStableChunkServerTimeoutTimes) {
        maxStableChunkServerTimeoutTimes_ = maxStableChunkServerTimeoutTimes;  // NOLINT
    }

 private:
    // 连续超时次数上限
    static uint64_t maxStableChunkServerTimeoutTimes_;

    // 保护timeoutTimes_;
    static SpinLock timeoutLock_;

    // 同一chunkserver连续超时请求次数
    static std::unordered_map<ChunkServerID, uint64_t> timeoutTimes_;
};

/**
 * ClientClosure，负责保存Rpc上下文，
 * 包含cntl和response已经重试次数
 */
class ClientClosure : public Closure {
 public:
    ClientClosure(CopysetClient *client, Closure *done):
                                        client_(client),
                                        done_(done) {
    }
    virtual void SetCntl(brpc::Controller* cntl) = 0;
    virtual void SetResponse(Message* response) = 0;
    virtual void SetRetriedTimes(uint16_t retriedTimes) = 0;
    virtual void SetChunkServerID(ChunkServerID csid) {
        chunkserverID_ = csid;
    }

    virtual ChunkServerID GetChunkServerID() {
        return chunkserverID_;
    }

    static void SetFailureRequestOption(
            const FailureRequestOption_t& failRequestOpt) {
        failReqOpt_ = failRequestOpt;

        UnstableChunkServerHelper::SetMaxStableChunkServerTimeoutTimes(
            failReqOpt_.maxStableChunkServerTimeoutTimes);

        std::srand(std::time(nullptr));
        SetBackoffParam();

        confMetric_.opMaxRetry.set_value(failReqOpt_.opMaxRetry);
        confMetric_.opRetryIntervalUs.set_value(
                    failReqOpt_.opRetryIntervalUs);
        DVLOG(9) << "Client clousre conf info: "
              << "opRetryIntervalUs = " << failReqOpt_.opRetryIntervalUs
              << ", opMaxRetry = " << failReqOpt_.opMaxRetry;
    }

    Closure* GetClosure() {
        return done_;
    }

    /**
     * 在重试之前根据返回值进行预处理
     * 场景1: rpc timeout，那么这时候会指数增加当前rpc的超时时间，然后直接进行重试
     * 场景2：底层OVERLOAD，那么需要在重试之前睡眠一段时间，睡眠时间根据重试次数指数增长
     * @param: rpcstatue为rpc返回值
     * @param: cntlstatus为本次rpc controller返回值
     */
    void PreProcessBeforeRetry(int rpcstatue, int cntlstatus);
    /**
     * 底层chunkserver overload之后需要根据重试次数进行退避
     * @param: currentRetryTimes为当前已重试的次数
     * @return: 返回当前的需要睡眠的时间
     */
    uint64_t OverLoadBackOff(uint64_t currentRetryTimes);
    /**
     * rpc timeout之后需要根据重试次数进行退避
     * @param: currentRetryTimes为当前已重试的次数
     * @return: 返回下一次RPC 超时时间
     */
    uint64_t TimeoutBackOff(uint64_t currentRetryTimes);

    struct BackoffParam {
        uint64_t maxTimeoutPow;
        uint64_t maxOverloadPow;
        BackoffParam() {
            maxTimeoutPow = 1;
            maxOverloadPow = 1;
        }
    };

    static void SetBackoffParam() {
        uint64_t overloadTimes = failReqOpt_.maxRetrySleepIntervalUs
                                / failReqOpt_.opRetryIntervalUs;
        backoffParam_.maxOverloadPow = GetPowTime(overloadTimes);


        uint64_t timeoutTimes = failReqOpt_.maxTimeoutMS
                             / failReqOpt_.rpcTimeoutMs;
        backoffParam_.maxTimeoutPow = GetPowTime(timeoutTimes);
    }

    static uint64_t GetPowTime(uint64_t value) {
        int pow = 0;
        while (value > 1) {
            value>>=1;
            pow++;
        }
        return pow;
    }

    static BackoffParam backoffParam_;

 protected:
    static FailureRequestOption_t  failReqOpt_;
    // 已经重试了几次
    uint64_t                 retriedTimes_;
    brpc::Controller*        cntl_;
    ChunkResponse*           response_;
    CopysetClient*           client_;
    Closure*                 done_;
    // 这里保存chunkserverID，是为了区别当前这个rpc是发给哪个chunkserver的
    // 这样方便在rpc closure里直接找到，当前是哪个chunkserver返回的失败
    ChunkServerID            chunkserverID_;
};

class WriteChunkClosure : public ClientClosure {
 public:
    WriteChunkClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class ReadChunkClosure : public ClientClosure {
 public:
    ReadChunkClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class ReadChunkSnapClosure : public ClientClosure {
 public:
    ReadChunkSnapClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class DeleteChunkSnapClosure : public ClientClosure {
 public:
    DeleteChunkSnapClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class GetChunkInfoClosure : public ClientClosure {
 public:
    GetChunkInfoClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        chunkinforesponse_ = dynamic_cast<GetChunkInfoResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    GetChunkInfoResponse*    chunkinforesponse_;
};

class CreateCloneChunkClosure : public ClientClosure {
 public:
    CreateCloneChunkClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class RecoverChunkClosure : public ClientClosure {
 public:
    RecoverChunkClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CHUNK_CLOSURE_H_
