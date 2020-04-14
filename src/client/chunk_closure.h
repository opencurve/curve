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
#include "src/client/service_helper.h"

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

enum class UnstableState {
    NoUnstable,
    ChunkServerUnstable,
    ServerUnstable
};

// 如果chunkserver宕机或者网络不可达, 发往对应chunkserver的rpc会超时
// 返回之后, 回去refresh leader然后再去发送请求
// 这种情况下不同copyset上的请求，总会先rpc timedout然后重新refresh leader
// 为了避免一次多余的rpc timedout
// 记录一下发往同一个chunkserver上超时请求的次数
// 如果超过一定的阈值，会发送http请求检查chunkserver是否健康
// 如果不健康，则通知所有leader在这台chunkserver上的copyset
// 主动去refresh leader，而不是根据缓存的leader信息直接发送rpc
class UnstableHelper {
 public:
    static UnstableHelper& GetInstance() {
        static UnstableHelper helper;
        return helper;
    }

    void IncreTimeout(ChunkServerID csId) {
        lock_.Lock();
        ++timeoutTimes_[csId];
        lock_.UnLock();
    }

    UnstableState GetCurrentUnstableState(
        ChunkServerID csId,
        const butil::EndPoint& csEndPoint);

    void ClearTimeout(ChunkServerID csId,
                      const butil::EndPoint& csEndPoint) {
        std::string ip = butil::ip2str(csEndPoint.ip).c_str();

        lock_.Lock();
        timeoutTimes_[csId] = 0;
        serverUnstabledChunkservers_[ip].erase(csId);
        lock_.UnLock();
    }

    void SetUnstableChunkServerOption(
        const ChunkServerUnstableOption& opt) {
        option_ = opt;
    }

    // 测试使用，重置计数器
    void ResetState() {
        timeoutTimes_.clear();
        serverUnstabledChunkservers_.clear();
    }

 private:
    UnstableHelper() = default;

    /**
     * @brief 检查chunkserver状态
     *
     * @param: endPoint chunkserver的ip:port地址
     * @return: true 健康 / false 不健康
     */
    bool CheckChunkServerHealth(const butil::EndPoint& endPoint) {
        return ServiceHelper::CheckChunkServerHealth(
            endPoint, option_.checkHealthTimeoutMS) == 0;
    }

    ChunkServerUnstableOption option_;

    SpinLock lock_;

    // 同一chunkserver连续超时请求次数
    std::unordered_map<ChunkServerID, uint32_t> timeoutTimes_;

    // 同一server上unstable chunkserver的id
    std::unordered_map<std::string, std::unordered_set<ChunkServerID>> serverUnstabledChunkservers_;  // NOLINT
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

    void SetChunkServerEndPoint(const butil::EndPoint& endPoint) {
        chunkserverEndPoint_ = endPoint;
    }

    EndPoint GetChunkServerEndPoint() const {
        return chunkserverEndPoint_;
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

        UnstableHelper::GetInstance().SetUnstableChunkServerOption(
            failReqOpt_.chunkserverUnstableOption);

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
    int UpdateLeaderWithRedirectInfo(const std::string& leaderInfo);

    void ProcessUnstableState();

    void RefreshLeader();

    static FailureRequestOption_t       failReqOpt_;

    brpc::Controller*                   cntl_;
    std::unique_ptr<ChunkResponse>      response_;
    CopysetClient*                      client_;
    Closure*                            done_;
    // 这里保存chunkserverID，是为了区别当前这个rpc是发给哪个chunkserver的
    // 这样方便在rpc closure里直接找到，当前是哪个chunkserver返回的失败
    ChunkServerID                       chunkserverID_;
    butil::EndPoint                     chunkserverEndPoint_;

    // 记录当前请求的相关信息
    MetaCache*                          metaCache_;
    RequestClosure*                     reqDone_;
    FileMetric*                         fileMetric_;
    RequestContext*                     reqCtx_;
    ChunkIDInfo                         chunkIdInfo_;

    // 发送重试请求前是否睡眠
    bool retryDirectly_{false};

    // response 状态码
    int                                 status_;

    // rpc 状态码
    int                                 cntlstatus_;

    // rpc remote side address
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
