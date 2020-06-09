/*
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "src/part1/nebd_client.h"

#include <unistd.h>
#include <sys/file.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <string>

#include "src/part1/async_request_closure.h"
#include "src/common/configuration.h"

#define RETURN_IF_FALSE(val) if (val == false) { return -1; }

// 修改brpc的health_check_interval参数，这个参数用来控制健康检查的周期
// ## 健康检查
// 连接断开的server会被暂时隔离而不会被负载均衡算法选中，brpc会定期连接被隔离的server，以检查他们是否恢复正常，间隔由参数-health_check_interval控制:   // NOLINT
// | Name                      | Value | Description                              | Defined At              |                                // NOLINT
// | ------------------------- | ----- | ---------------------------------------- | ----------------------- |                                // NOLINT
// | health_check_interval （R） | 3     | seconds between consecutive health-checkings | src/brpc/socket_map.cpp |                           // NOLINT
// 一旦server被连接上，它会恢复为可用状态。如果在隔离过程中，server从命名服务中删除了，brpc也会停止连接尝试。                                         // NOLINT
namespace brpc {
    DECLARE_int32(health_check_interval);
    DECLARE_int32(circuit_breaker_max_isolation_duration_ms);
}  // namespace brpc

namespace nebd {
namespace client {

using nebd::common::FileLock;

NebdClient &nebdClient = NebdClient::GetInstance();

constexpr int32_t kBufSize = 128;

int NebdClient::Init(const char* confpath) {
    nebd::common::Configuration conf;
    conf.SetConfigPath(confpath);

    if (!conf.LoadConfig()) {
        LOG(ERROR) << "Load config failed, conf path = " << confpath;
        return -1;
    }

    int ret = InitNebdClientOption(&conf);
    if (ret != 0) {
        LOG(ERROR) << "InitNebdClientOption failed";
        return -1;
    }

    // init glog
    InitLogger(option_.logOption);

    HeartbeatOption heartbeatOption;
    ret = InitHeartBeatOption(&conf, &heartbeatOption);
    if (ret != 0) {
        LOG(ERROR) << "InitHeartBeatOption failed";
        return -1;
    }

    LOG(INFO) << "Load config success!";

    ret = InitChannel();
    if (ret != 0) {
        LOG(ERROR) << "InitChannel failed";
        return -1;
    }

    metaCache_ = std::make_shared<NebdClientMetaCache>();
    heartbeatMgr_ = std::make_shared<HeartbeatManager>(
        metaCache_);

    ret = heartbeatMgr_->Init(heartbeatOption);
    if (ret != 0) {
        LOG(ERROR) << "Heartbeat Manager InitChannel failed";
        return -1;
    }

    heartbeatMgr_->Run();

    return 0;
}

void NebdClient::Uninit() {
    if (heartbeatMgr_ != nullptr) {
        heartbeatMgr_->Stop();
    }
    LOG(INFO) << "NebdClient uninit success.";
    google::ShutdownGoogleLogging();
}

int NebdClient::Open(const char* filename) {
    // 加文件锁
    std::string fileLockName =
        option_.fileLockPath + "/" + ReplaceSlash(filename);
    FileLock fileLock(fileLockName);
    int res = fileLock.AcquireFileLock();
    if (res < 0) {
        LOG(ERROR) << "Open file failed when AcquireFileLock, filename = "
                   << filename;
        return -1;
    }

    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        NebdFileService_Stub stub(channel);
        OpenFileRequest request;
        OpenFileResponse response;

        request.set_filename(filename);
        stub.OpenFile(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "OpenFile rpc failed, error = "
                         << cntl->ErrorText()
                         << ", filename = " << filename
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != RetCode::kOK) {
                LOG(ERROR) << "OpenFile failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", filename = " << filename
                           << ", log id = " << cntl->log_id();
                return -1;
            }

            return response.fd();
        }
    };

    int fd = ExecuteSyncRpc(task);
    if (fd < 0) {
        LOG(ERROR) << "Open file failed, filename = " << filename;
        fileLock.ReleaseFileLock();
        return -1;
    }

    metaCache_->AddFileInfo({fd, filename, fileLock});
    return fd;
}

int NebdClient::Close(int fd) {
    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        NebdFileService_Stub stub(channel);
        CloseFileRequest request;
        CloseFileResponse response;

        request.set_fd(fd);
        stub.CloseFile(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "CloseFile rpc failed, error = "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != RetCode::kOK) {
                LOG(ERROR) << "CloseFile failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", fd = " << fd
                           << ", log id = " << cntl->log_id();
            }

            return 0;
        }
    };

    int rpcRet = ExecuteSyncRpc(task);
    NebdClientFileInfo fileInfo;
    int ret = metaCache_->GetFileInfo(fd, &fileInfo);
    if (ret == 0) {
        fileInfo.fileLock.ReleaseFileLock();
        metaCache_->RemoveFileInfo(fd);
    }

    return rpcRet;
}

int NebdClient::Extend(int fd, int64_t newsize) {
    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        nebd::client::NebdFileService_Stub stub(&channel_);
        nebd::client::ResizeRequest request;
        nebd::client::ResizeResponse response;

        request.set_fd(fd);
        request.set_newsize(newsize);

        stub.ResizeFile(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "Resize RPC failed, error = "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "ExtendFile failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", fd = " << fd
                           << ", newsize = " << newsize
                           << ", log id = " << cntl->log_id();
                return -1;
            } else {
                return 0;
            }
        }
    };

    int64_t ret = ExecuteSyncRpc(task);
    if (ret < 0) {
        LOG(ERROR) << "Extend failed, fd = " << fd
                   << ", newsize = " << newsize;
    }
    return ret;
}

int64_t NebdClient::GetFileSize(int fd) {
    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        nebd::client::NebdFileService_Stub stub(channel);
        nebd::client::GetInfoRequest request;
        nebd::client::GetInfoResponse response;

        request.set_fd(fd);
        stub.GetInfo(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "GetFileSize faield, error = "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "GetFileSize failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", fd = " << fd
                           << ", log id = " << cntl->log_id();
                return -1;
            } else {
                return response.info().size();
            }
        }
    };

    int64_t ret = ExecuteSyncRpc(task);
    if (ret < 0) {
        LOG(ERROR) << "GetFileSize failed, fd = " << fd;
    }
    return ret;
}

int NebdClient::Discard(int fd, NebdClientAioContext* aioctx) {
    nebd::client::NebdFileService_Stub stub(&channel_);
    nebd::client::DiscardRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);

    AioDiscardClosure* done = new(std::nothrow) AioDiscardClosure(
        fd, aioctx, option_.requestOption);
    done->cntl.set_timeout_ms(-1);
    done->cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));
    stub.Discard(&done->cntl, &request, &done->response, done);

    return 0;
}

int NebdClient::AioRead(int fd, NebdClientAioContext* aioctx) {
    nebd::client::NebdFileService_Stub stub(&channel_);
    nebd::client::ReadRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);

    AioReadClosure* done = new(std::nothrow) AioReadClosure(
        fd, aioctx, option_.requestOption);
    done->cntl.set_timeout_ms(-1);
    done->cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));
    stub.Read(&done->cntl, &request, &done->response, done);
    return 0;
}

static void EmptyDeleter(void* m) {}

int NebdClient::AioWrite(int fd, NebdClientAioContext* aioctx) {
    nebd::client::NebdFileService_Stub stub(&channel_);
    nebd::client::WriteRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);

    AioWriteClosure* done = new(std::nothrow) AioWriteClosure(
        fd, aioctx, option_.requestOption);

    done->cntl.set_timeout_ms(-1);
    done->cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));
    done->cntl.request_attachment().append_user_data(
        aioctx->buf, aioctx->length, EmptyDeleter);
    stub.Write(&done->cntl, &request, &done->response, done);

    return 0;
}

int NebdClient::Flush(int fd, NebdClientAioContext* aioctx) {
    nebd::client::NebdFileService_Stub stub(&channel_);
    nebd::client::FlushRequest request;
    request.set_fd(fd);

    AioFlushClosure* done = new(std::nothrow) AioFlushClosure(
        fd, aioctx, option_.requestOption);
    done->cntl.set_timeout_ms(-1);
    done->cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));
    stub.Flush(&done->cntl, &request, &done->response, done);
    return 0;
}

int64_t NebdClient::GetInfo(int fd) {
    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        nebd::client::NebdFileService_Stub stub(channel);
        nebd::client::GetInfoRequest request;
        nebd::client::GetInfoResponse response;

        request.set_fd(fd);
        stub.GetInfo(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "GetInfo rpc failed, error = "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "GetInfo failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", fd = " << fd
                           << ", log id = " << cntl->log_id();
                return -1;
            } else {
                return response.info().objsize();
            }
        }
    };

    int64_t ret = ExecuteSyncRpc(task);
    if (ret < 0) {
        LOG(ERROR) << "GetInfo failed, fd = " << fd;
    }
    return ret;
}

int NebdClient::InvalidCache(int fd) {
    auto task = [&](brpc::Controller* cntl,
                    brpc::Channel* channel,
                    bool* rpcFailed) -> int64_t {
        nebd::client::NebdFileService_Stub stub(channel);
        nebd::client::InvalidateCacheRequest request;
        nebd::client::InvalidateCacheResponse response;

        request.set_fd(fd);
        stub.InvalidateCache(cntl, &request, &response, nullptr);

        *rpcFailed = cntl->Failed();
        if (*rpcFailed) {
            LOG(WARNING) << "InvalidCache rpc failed, error = "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -1;
        } else {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "InvalidCache failed, "
                           << "retcode = " << response.retcode()
                           <<",  retmsg = " << response.retmsg()
                           << ", fd = " << fd
                           << ", log id = " << cntl->log_id();
                return -1;
            } else {
                return 0;
            }
        }
    };

    int64_t ret = ExecuteSyncRpc(task);
    if (ret < 0) {
        LOG(ERROR) << "InvalidCache failed, fd = " << fd;
    }
    return ret;
}

int NebdClient::InitNebdClientOption(Configuration* conf) {
    bool ret = false;
    ret = conf->GetStringValue("nebdserver.serverAddress",
                               &option_.serverAddress);
    LOG_IF(ERROR, ret != true) << "Load nebdserver.serverAddress failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetStringValue("metacache.fileLockPath",
                               &option_.fileLockPath);
    LOG_IF(ERROR, ret != true) << "Load metacache.fileLockPath failed";
    RETURN_IF_FALSE(ret);

    RequestOption requestOption;

    ret = conf->GetInt64Value("request.syncRpcMaxRetryTimes",
                              &requestOption.syncRpcMaxRetryTimes);
    LOG_IF(ERROR, ret != true) << "Load request.syncRpcMaxRetryTimes failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("request.rpcRetryIntervalUs",
                              &requestOption.rpcRetryIntervalUs);
    LOG_IF(ERROR, ret != true) << "Load request.rpcRetryIntervalUs failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("request.rpcRetryMaxIntervalUs",
                              &requestOption.rpcRetryMaxIntervalUs);
    LOG_IF(ERROR, ret != true) << "Load request.rpcRetryMaxIntervalUs failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("request.rpcHostDownRetryIntervalUs",
                              &requestOption.rpcHostDownRetryIntervalUs);
    LOG_IF(ERROR, ret != true) << "Load request.rpcHostDownRetryIntervalUs failed";  // NOLINT
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("request.rpcHealthCheckIntervalS",
                              &requestOption.rpcHealthCheckIntervalS);
    LOG_IF(ERROR, ret != true) << "Load request.rpcHealthCheckIntervalS failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("request.rpcMaxDelayHealthCheckIntervalMs",
                              &requestOption.rpcMaxDelayHealthCheckIntervalMs);
    LOG_IF(ERROR, ret != true) << "Load request.rpcMaxDelayHealthCheckIntervalMs failed";  // NOLINT
    RETURN_IF_FALSE(ret);

    option_.requestOption = requestOption;

    ret = conf->GetStringValue("log.path", &option_.logOption.logPath);
    LOG_IF(ERROR, ret != true) << "Load log.path failed";
    RETURN_IF_FALSE(ret);

    return 0;
}

int NebdClient::InitHeartBeatOption(Configuration* conf,
                                    HeartbeatOption* heartbeatOption) {
    bool ret = conf->GetInt64Value("heartbeat.intervalS",
                                   &heartbeatOption->intervalS);
    LOG_IF(ERROR, ret != true) << "Load heartbeat.intervalS failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetInt64Value("heartbeat.rpcTimeoutMs",
                              &heartbeatOption->rpcTimeoutMs);
    LOG_IF(ERROR, ret != true) << "Load heartbeat.rpcTimeoutMs failed";
    RETURN_IF_FALSE(ret);

    ret = conf->GetStringValue("nebdserver.serverAddress",
                               &heartbeatOption->serverAddress);
    LOG_IF(ERROR, ret != true) << "Load nebdserver.serverAddress failed";
    RETURN_IF_FALSE(ret);

    return 0;
}

int NebdClient::InitChannel() {
    brpc::FLAGS_health_check_interval =
        option_.requestOption.rpcHealthCheckIntervalS;
    brpc::FLAGS_circuit_breaker_max_isolation_duration_ms =
        option_.requestOption.rpcMaxDelayHealthCheckIntervalMs;
    int ret = channel_.InitWithSockFile(
        option_.serverAddress.c_str(), nullptr);
    if (ret != 0) {
        LOG(ERROR) << "Init Channel failed, socket addr = "
                   << option_.serverAddress;
        return -1;
    }

    return 0;
}

int64_t NebdClient::ExecuteSyncRpc(RpcTask task) {
    int64_t retryTimes = 0;
    int64_t ret = 0;

    while (retryTimes++ < option_.requestOption.syncRpcMaxRetryTimes) {
        brpc::Controller cntl;

        cntl.set_timeout_ms(-1);
        cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));

        bool rpcFailed = false;
        ret = task(&cntl, &channel_, &rpcFailed);
        if (rpcFailed) {
            bthread_usleep(option_.requestOption.rpcRetryIntervalUs);
            continue;
        } else {
            return ret;
        }
    }

    LOG(ERROR) << "retried " << retryTimes << " times, max retry times "
               << option_.requestOption.syncRpcMaxRetryTimes;

    return -1;
}

std::string NebdClient::ReplaceSlash(const std::string& str) {
    std::string ret(str);
    for (auto& ch : ret) {
        if (ch == '/') {
            ch = '+';
        }
    }

    return ret;
}


void NebdClient::InitLogger(const LogOption& logOption) {
    static const char* kProcessName = "nebd-client";

    FLAGS_log_dir = logOption.logPath;
    FLAGS_stderrthreshold = 3;
    google::InitGoogleLogging(kProcessName);
}

}  // namespace client
}  // namespace nebd
