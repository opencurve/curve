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
 * Created Date: 18-9-27
 * Author: wudemiao
 */

#ifndef SRC_CLIENT_CHUNK_CLOSURE_H_
#define SRC_CLIENT_CHUNK_CLOSURE_H_

#include <brpc/controller.h>
#include <brpc/errno.pb.h>
#include <google/protobuf/stubs/callback.h>

#include <memory>
#include <string>

#include "proto/chunk.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/client/request_closure.h"
#include "src/common/math_util.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoResponse;
using ::google::protobuf::Closure;
using ::google::protobuf::Message;

class MetaCache;
class CopysetClient;

/**
 * ClientClosure, responsible for maintaining the Rpc context,
 * including the control (cntl) and response, as well as the
 * retry count.
 */
class ClientClosure : public Closure {
 public:
    ClientClosure(CopysetClient* client, Closure* done)
        : client_(client), done_(done) {}

    virtual ~ClientClosure() = default;

    void SetCntl(brpc::Controller* cntl) { cntl_ = cntl; }

    virtual void SetResponse(Message* response) {
        response_.reset(static_cast<ChunkResponse*>(response));
    }

    void SetChunkServerID(ChunkServerID csid) { chunkserverID_ = csid; }

    ChunkServerID GetChunkServerID() const { return chunkserverID_; }

    void SetChunkServerEndPoint(const butil::EndPoint& endPoint) {
        chunkserverEndPoint_ = endPoint;
    }

    EndPoint GetChunkServerEndPoint() const { return chunkserverEndPoint_; }

    // Unified Run Function Entry
    void Run() override;

    // Retrying the request
    void OnRetry();

    // Rpc Failed processing function
    void OnRpcFailed();

    // Return successful processing function
    virtual void OnSuccess();

    // Return redirection processing function
    virtual void OnRedirected();

    // copyset does not exist
    void OnCopysetNotExist();

    // Return backward
    void OnBackward();

    // Returning chunk with no processing function present
    virtual void OnChunkNotExist();

    // Return Chunk Existence Processing Function
    void OnChunkExist();

    // handle epoch too old
    void OnEpochTooOld();

    // Illegal parameter
    void OnInvalidRequest();

    // Send retry request
    virtual void SendRetryRequest() = 0;

    // Obtain the status code returned by the response
    virtual CHUNK_OP_STATUS GetResponseStatus() const {
        return response_->status();
    }

    static void SetFailureRequestOption(
        const FailureRequestOption& failRequestOpt) {
        failReqOpt_ = failRequestOpt;

        std::srand(std::time(nullptr));
        SetBackoffParam();

        DVLOG(9) << "Client clousre conf info: "
                 << "chunkserverOPRetryIntervalUS = "
                 << failReqOpt_.chunkserverOPRetryIntervalUS
                 << ", chunkserverOPMaxRetry = "
                 << failReqOpt_.chunkserverOPMaxRetry;
    }

    Closure* GetClosure() const { return done_; }

    // Test usage, set closure
    void SetClosure(Closure* done) { done_ = done; }

    static FailureRequestOption GetFailOpt() { return failReqOpt_; }

    /**
     * Preprocess based on the return value before retrying.
     * Scenario 1: RPC timeout results in exponentially increasing
     * the current RPC timeout and retrying immediately.
     * Scenario 2: Underlying OVERLOAD condition requires sleeping
     * for a period of time before retrying, where the sleep time
     * exponentially increases based on the retry count.
     * @param rpcstatus: Return value of the RPC.
     * @param cntlstatus: Return value of the RPC controller for this instance.
     */
    void PreProcessBeforeRetry(int rpcstatue, int cntlstatus);

    /**
     * After underlying chunkserver overload, backoff is required
     * based on the retry count.
     * @param currentRetryTimes: The current number of retries.
     * @return: Returns the current sleep time needed.
     */
    static uint64_t OverLoadBackOff(uint64_t currentRetryTimes);

    /**
     * After RPC timeout, backoff is required based on the retry count.
     * @param currentRetryTimes: The current number of retries.
     * @return: Returns the next RPC timeout duration.
     */
    static uint64_t TimeoutBackOff(uint64_t currentRetryTimes);

    struct BackoffParam {
        uint64_t maxTimeoutPow;
        uint64_t maxOverloadPow;
        BackoffParam() {
            maxTimeoutPow = 1;
            maxOverloadPow = 1;
        }
    };

    static void SetBackoffParam() {
        using curve::common::MaxPowerTimesLessEqualValue;

        uint64_t overloadTimes =
            failReqOpt_.chunkserverMaxRetrySleepIntervalUS /
            failReqOpt_.chunkserverOPRetryIntervalUS;

        backoffParam_.maxOverloadPow =
            MaxPowerTimesLessEqualValue(overloadTimes);

        uint64_t timeoutTimes = failReqOpt_.chunkserverMaxRPCTimeoutMS /
                                failReqOpt_.chunkserverRPCTimeoutMS;
        backoffParam_.maxTimeoutPow = MaxPowerTimesLessEqualValue(timeoutTimes);
    }

    static BackoffParam backoffParam_;

 protected:
    int UpdateLeaderWithRedirectInfo(const std::string& leaderInfo);

    void ProcessUnstableState();

    void RefreshLeader();

    static FailureRequestOption failReqOpt_;

    brpc::Controller* cntl_;
    std::unique_ptr<ChunkResponse> response_;
    CopysetClient* client_;
    Closure* done_;

    // Saving the Chunkserver ID here is to distinguish which Chunkserver
    // this RPC is sent to. This makes it convenient to identify, within
    // the RPC closure, which Chunkserver returned a failure.

    ChunkServerID chunkserverID_;
    butil::EndPoint chunkserverEndPoint_;

    // Record relevant information for the current request
    MetaCache* metaCache_;
    RequestClosure* reqDone_;
    FileMetric* fileMetric_;
    RequestContext* reqCtx_;
    ChunkIDInfo chunkIdInfo_;

    // Whether to sleep before sending a retry request
    bool retryDirectly_ = false;

    // response status code
    int status_;

    // rpc status code
    int cntlstatus_;
};

class WriteChunkClosure : public ClientClosure {
 public:
    WriteChunkClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void OnSuccess() override;
    void SendRetryRequest() override;
};

class ReadChunkClosure : public ClientClosure {
 public:
    ReadChunkClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void OnSuccess() override;
    void OnChunkNotExist() override;
    void SendRetryRequest() override;
};

class ReadChunkSnapClosure : public ClientClosure {
 public:
    ReadChunkSnapClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void OnSuccess() override;
    void SendRetryRequest() override;
};

class DeleteChunkSnapClosure : public ClientClosure {
 public:
    DeleteChunkSnapClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

class GetChunkInfoClosure : public ClientClosure {
 public:
    GetChunkInfoClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void SetResponse(Message* message) override {
        chunkinforesponse_.reset(static_cast<GetChunkInfoResponse*>(message));
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
    CreateCloneChunkClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

class RecoverChunkClosure : public ClientClosure {
 public:
    RecoverChunkClosure(CopysetClient* client, Closure* done)
        : ClientClosure(client, done) {}

    void SendRetryRequest() override;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_CHUNK_CLOSURE_H_
