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
#include <memory>
#include <string>

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
        uint64_t chunkserverMaxStableTimeoutTimes) {
        maxStableChunkServerTimeoutTimes_ = chunkserverMaxStableTimeoutTimes;
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
    ClientClosure(CopysetClient *client, Closure *done)
     : client_(client), done_(done) {}

    virtual ~ClientClosure() = default;

    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }

    virtual void SetResponse(Message* response) {
        response_.reset(dynamic_cast<ChunkResponse*>(response));
    }

    void SetChunkServerID(ChunkServerID csid) {
        chunkserverID_ = csid;
    }

    ChunkServerID GetChunkServerID() const {
        return chunkserverID_;
    }

    // 统一Run函数入口
    void Run() override;

    // 重试请求
    void OnRetry();

    // Rpc Failed 处理函数
    void OnRpcFailed();

    // 返回成功 处理函数
    virtual void OnSuccess();

    // 返回重定向 处理函数
    virtual void OnRedirected();

    // copyset不存在
    void OnCopysetNotExist();

    // 返回backward
    void OnBackward();

    // 返回chunk不存在 处理函数
    virtual void OnChunkNotExist();

    // 非法参数
    void OnInvalidRequest();

    // 发送重试请求
    virtual void SendRetryRequest() = 0;

    // 获取response返回的状态码
    virtual CHUNK_OP_STATUS GetResponseStatus() const {
        return response_->status();
    }

    static void SetFailureRequestOption(
            const FailureRequestOption_t& failRequestOpt) {
        failReqOpt_ = failRequestOpt;

        UnstableChunkServerHelper::SetMaxStableChunkServerTimeoutTimes(
            failReqOpt_.chunkserverMaxStableTimeoutTimes);

        std::srand(std::time(nullptr));
        SetBackoffParam();

        DVLOG(9) << "Client clousre conf info: "
              << "chunkserverOPRetryIntervalUS = "
              << failReqOpt_.chunkserverOPRetryIntervalUS
              << ", chunkserverOPMaxRetry = "
              << failReqOpt_.chunkserverOPMaxRetry;
    }

    Closure* GetClosure() const {
        return done_;
    }

    static FailureRequestOption_t GetFailOpt() {
        return failReqOpt_;
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
        uint64_t overloadTimes = failReqOpt_.chunkserverMaxRetrySleepIntervalUS
                                / failReqOpt_.chunkserverOPRetryIntervalUS;
        backoffParam_.maxOverloadPow = GetPowTime(overloadTimes);


        uint64_t timeoutTimes = failReqOpt_.chunkserverMaxRPCTimeoutMS
                             / failReqOpt_.chunkserverRPCTimeoutMS;
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
    void RefreshLeader() const;

    static FailureRequestOption_t       failReqOpt_;

    brpc::Controller*                   cntl_;
    std::unique_ptr<ChunkResponse>      response_;
    CopysetClient*                      client_;
    Closure*                            done_;
    // 这里保存chunkserverID，是为了区别当前这个rpc是发给哪个chunkserver的
    // 这样方便在rpc closure里直接找到，当前是哪个chunkserver返回的失败
    ChunkServerID                       chunkserverID_;

    // 记录当前请求的相关信息
    MetaCache*                          metaCache_;
    RequestClosure*                     reqDone_;
    FileMetric*                         fileMetric_;
    RequestContext*                     reqCtx_;
    ChunkIDInfo                         chunkIdInfo_;

    // response 状态码
    int                                 status_;

    // rpc 状态码
    int                                 cntlstatus_;

    // cntl remote side address
    std::string                         remoteAddress_;
};

class WriteChunkClosure : public ClientClosure {
 public:
    WriteChunkClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void OnSuccess() override;
    void SendRetryRequest() override;
};

class ReadChunkClosure : public ClientClosure {
 public:
    ReadChunkClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void OnSuccess() override;
    void OnChunkNotExist() override;
    void SendRetryRequest() override;
};

class ReadChunkSnapClosure : public ClientClosure {
 public:
    ReadChunkSnapClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void OnSuccess() override;
    void SendRetryRequest() override;
};

class DeleteChunkSnapClosure : public ClientClosure {
 public:
    DeleteChunkSnapClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

class GetChunkInfoClosure : public ClientClosure {
 public:
    GetChunkInfoClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void SetResponse(Message* message) override {
        chunkinforesponse_.reset(dynamic_cast<GetChunkInfoResponse*>(message));
    }

    CHUNK_OP_STATUS GetResponseStatus() const override {
        return chunkinforesponse_->status();
    }

    void OnSuccess() override;
    void OnRedirected() override;
    void SendRetryRequest() override;

 private:
    std::unique_ptr<GetChunkInfoResponse> chunkinforesponse_;
};

class CreateCloneChunkClosure : public ClientClosure {
 public:
    CreateCloneChunkClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

class RecoverChunkClosure : public ClientClosure {
 public:
    RecoverChunkClosure(CopysetClient *client, Closure *done)
     : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CHUNK_CLOSURE_H_
