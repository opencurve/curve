/*
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "src/part1/nebd_client.h"
#include <unistd.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <string>
#include "src/common/configuration.h"

// 修改brpc的health_check_interval参数，这个参数用来控制健康检查的周期
// ## 健康检查
// 连接断开的server会被暂时隔离而不会被负载均衡算法选中，brpc会定期连接被隔离的server，以检查他们是否恢复正常，间隔由参数-health_check_interval控制:   // NOLINT
// | Name                      | Value | Description                              | Defined At              |                                // NOLINT
// | ------------------------- | ----- | ---------------------------------------- | ----------------------- |                                // NOLINT
// | health_check_interval （R） | 3     | seconds between consecutive health-checkings | src/brpc/socket_map.cpp |                           // NOLINT
// 一旦server被连接上，它会恢复为可用状态。如果在隔离过程中，server从命名服务中删除了，brpc也会停止连接尝试。                                         // NOLINT
namespace brpc {
    DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace nebd {
namespace client {
FileClient &fileClient = FileClient::GetInstance();

int FileClient::Init(const char* confpath) {
    // 从配置文件中获取
    using ::nebd::common::Configuration;
    Configuration conf;
    conf.SetConfigPath(confpath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "load conf fail, conf path = " << confpath;
        return -1;
    }

    int ret = LoadConf(&conf);
    if (ret != 0) {
        LOG(ERROR) << "FileClient LoadConf fail.";
        return -1;
    }

    ret = lifeCycleManager_.Start(&conf);
    if (ret != 0) {
        LOG(ERROR) << "FileClient lifecycle manager start fail";
        return -1;
    }

    std::string addr = lifeCycleManager_.GetPart2Addr();
    ret = InitChannel(addr);
    if (ret != 0) {
        LOG(ERROR) << "FileClient init channel fail, addr = " << addr;
        return -1;
    }

    lifeCycleManager_.StartHeartbeat();

    LOG(INFO) << "FileClient init success.";
    return 0;
}

void FileClient::Uninit() {
    lifeCycleManager_.Stop();
    LOG(INFO) << "FileClient uninit success.";
}

int FileClient::Open(const char* filename) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::OpenFileRequest request;
        nebd::client::OpenFileResponse response;
        request.set_filename(filename);
        stub.OpenFile(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "Open fail, retCode = "
                           << response.retcode()
                           << ", filename = " << filename;
                return -1;
            }
            std::string retMsg = response.retmsg();
            uint64_t fd = response.fd();
            LOG(INFO) << "Open ok." << "retMsg: " << retMsg
                      << ", filename = " << filename
                      << ", fd = " << fd;
            return fd;
        } else {
            LOG(WARNING) << "Open fail, filename = " << filename
                         << ", cntl errorCode: " << cntl.ErrorCode()
                         << ", cntl error: " << cntl.ErrorText();
        }
        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "Open fail, filename = " << filename
               << ", retryCount = " << retryCount;
    return -1;
}

int FileClient::Close(int fd) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::CloseFileRequest request;
        nebd::client::CloseFileResponse response;
        request.set_fd(fd);
        stub.CloseFile(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "Close fail, retCode = "
                           << response.retcode()
                           << ", fd = " << fd;
                return -1;
            }
            std::string retMsg = response.retmsg();
            LOG(INFO) << "Close ok." << "retMsg: " << retMsg
                      << ", fd = " << fd;
            return 0;
        } else {
            LOG(ERROR) << "Close fail, fd = " << fd
                       << ", cntl errorCode: " << cntl.ErrorCode()
                       << ", cntl error: " << cntl.ErrorText();
        }
        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "Close fail, fd = " << fd
               << ", retryCount = " << retryCount;
    return -1;
}

int FileClient::Extend(int fd, int64_t newsize) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::ResizeRequest request;
        nebd::client::ResizeResponse response;
        request.set_fd(fd);
        request.set_newsize(newsize);
        stub.ResizeFile(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "Extend fail, retCode = "
                           << response.retcode()
                           << ", fd = " << fd
                           << ", newsize = " << newsize;
                return -1;
            }
            std::string retMsg = response.retmsg();
            LOG(INFO) << "Extend ok." << "retMsg: " << retMsg
                      << ", fd = " << fd
                      << ", newsize = " << newsize;
            return 0;
        } else {
            LOG(ERROR) << "Extend fail, fd = " << fd
                       << ", newsize = " << newsize
                       << ", cntl errorCode: " << cntl.ErrorCode()
                       << ", cntl error: " << cntl.ErrorText();
        }

        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "Extend fail, fd = " << fd
               << ", newsize = " << newsize
               << ", retryCount = " << retryCount;
    return -1;
}

int64_t FileClient::StatFile(int fd) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::StatFileRequest request;
        nebd::client::StatFileResponse response;
        request.set_fd(fd);
        stub.StatFile(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "StatFile fail, retCode = "
                           << response.retcode()
                           << ", fd = " << fd;
                return -1;
            }
            std::string retMsg = response.retmsg();
            uint64_t size = response.size();
            LOG(INFO) << "StatFile ok." << "retMsg: " << retMsg
                      << ", fd = " << fd
                      << ", size = " << size;
            return size;
        } else {
            LOG(ERROR) << "StatFile fail, fd = " << fd
                       << ", cntl errorCode: " << cntl.ErrorCode()
                       << ", cntl error: " << cntl.ErrorText();
        }

        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "StatFile fail, fd = " << fd
               << ", retryCount = " << retryCount;
    return -1;
}

int FileClient::Discard(int fd, ClientAioContext* aioctx) {
    DVLOG(6) << "Discard start, fd = " << fd
              << ", offset = " << aioctx->offset
              << ", length = " << aioctx->length;
    nebd::client::QemuClientService_Stub stub(&channel_);
    nebd::client::DiscardRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);
    DiscardDone *discardDone = new DiscardDone(fd, aioctx, rpcTimeoutMs_,
                            rpcRetryIntervalUs_, rpcRetryMaxIntervalUs_,
                            rpcHostDownRetryIntervalUs_);
    stub.Discard(&discardDone->cntl_, &request,
                    &discardDone->response_, discardDone);
    return 0;
}

int FileClient::AioRead(int fd, ClientAioContext* aioctx) {
    DVLOG(6) << "AioRead start, fd = " << fd
              << ", offset = " << aioctx->offset
              << ", length = " << aioctx->length;
    nebd::client::QemuClientService_Stub stub(&channel_);
    nebd::client::ReadRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);
    ReadDone *readDone = new ReadDone(fd, aioctx, rpcTimeoutMs_,
                            rpcRetryIntervalUs_, rpcRetryMaxIntervalUs_,
                            rpcHostDownRetryIntervalUs_);
    stub.Read(&readDone->cntl_, &request, &readDone->response_, readDone);
    return 0;
}

int FileClient::AioWrite(int fd, ClientAioContext* aioctx) {
    DVLOG(6) << "AioWrite start, fd = " << fd
              << ", offset = " << aioctx->offset
              << ", length = " << aioctx->length;
    nebd::client::QemuClientService_Stub stub(&channel_);
    nebd::client::WriteRequest request;
    request.set_fd(fd);
    request.set_offset(aioctx->offset);
    request.set_size(aioctx->length);
    WriteDone *writeDone = new WriteDone(fd, aioctx, rpcTimeoutMs_,
                            rpcRetryIntervalUs_, rpcRetryMaxIntervalUs_,
                            rpcHostDownRetryIntervalUs_);
    writeDone->cntl_.request_attachment().append(aioctx->buf, aioctx->length);
    stub.Write(&writeDone->cntl_, &request, &writeDone->response_, writeDone);
    return 0;
}

int FileClient::Flush(int fd, ClientAioContext* aioctx) {
    DVLOG(6) << "Flush start, fd = " << fd;
    nebd::client::QemuClientService_Stub stub(&channel_);
    nebd::client::FlushRequest request;
    request.set_fd(fd);
    FlushDone *flushDone = new FlushDone(fd, aioctx, rpcTimeoutMs_,
                            rpcRetryIntervalUs_, rpcRetryMaxIntervalUs_,
                            rpcHostDownRetryIntervalUs_);
    stub.Flush(&flushDone->cntl_, &request, &flushDone->response_, flushDone);
    return 0;
}

int64_t FileClient::GetInfo(int fd) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::GetInfoRequest request;
        nebd::client::GetInfoResponse response;
        request.set_fd(fd);
        stub.GetInfo(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "GetInfo fail, retCode = "
                           << response.retcode()
                           << ", fd = " << fd;
                return -1;
            }
            std::string retMsg = response.retmsg();
            uint64_t size = response.objsize();
            LOG(INFO) << "GetInfo ok." << "retMsg: " << retMsg
                      << ", fd = " << fd
                      << ", size = " << size;
            return size;
        } else {
            LOG(ERROR) << "GetInfo fail, fd = " << fd
                       << ", cntl errorCode: " << cntl.ErrorCode()
                       << ", cntl error: " << cntl.ErrorText();
        }

        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "GetInfo fail, fd = " << fd
               << ", retryCount = " << retryCount;
    return -1;
}

int FileClient::InvalidCache(int fd) {
    uint32_t retryCount = 0;
    do {
        nebd::client::QemuClientService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(rpcTimeoutMs_);
        nebd::client::InvalidateCacheRequest request;
        nebd::client::InvalidateCacheResponse response;
        request.set_fd(fd);
        stub.InvalidateCache(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (response.retcode() != nebd::client::RetCode::kOK) {
                LOG(ERROR) << "InvalidateCache fail, retCode = "
                           << response.retcode()
                           << ", fd = " << fd;
                return -1;
            }
            std::string retMsg = response.retmsg();
            LOG(INFO) << "InvalidateCache ok." << "retMsg: " << retMsg
                      << ", fd = " << fd;
            return 0;
        } else {
            LOG(ERROR) << "InvalidateCache fail, fd = " << fd
                       << ", cntl errorCode: " << cntl.ErrorCode()
                       << ", cntl error: " << cntl.ErrorText();
        }

        retryCount++;
        bthread_usleep(rpcRetryIntervalUs_);
    } while (retryCount < rpcRetryTimes_);

    LOG(ERROR) << "InvalidateCache fail, fd = " << fd
               << ", retryCount = " << retryCount;
    return -1;
}

int FileClient::LoadConf(common::Configuration *conf) {
    if (!conf->GetUInt32Value("rpcRetryTimes", &rpcRetryTimes_)) {
        LOG(ERROR) << "get rpcRetryTimes fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("rpcRetryIntervalUs", &rpcRetryIntervalUs_)) {
        LOG(ERROR) << "get rpcRetryIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("rpcHostDownRetryIntervalUs",
                            &rpcHostDownRetryIntervalUs_)) {
        LOG(ERROR) << "get rpcHostDownRetryIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("rpcRetryMaxIntervalUs",
                            &rpcRetryMaxIntervalUs_)) {
        LOG(ERROR) << "get rpcRetryMaxIntervalUs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("rpcTimeoutMs", &rpcTimeoutMs_)) {
        LOG(ERROR) << "get rpcTimeoutMs fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("rpcHealthCheckIntervalS",
                            &rpcHealthCheckIntervalS_)) {
        LOG(ERROR) << "get rpcHealthCheckIntervalS fail.";
        return -1;
    }

    if (!conf->GetUInt32Value("aioRpcFailLogInterval",
                            &aioRpcFailLogInterval_)) {
        LOG(ERROR) << "get aioRpcFailLogInterval fail.";
        return -1;
    }

    LOG(INFO) << "FileClient load conf success.";

    return 0;
}

int FileClient::InitChannel(const std::string& addr) {
    brpc::FLAGS_health_check_interval = rpcHealthCheckIntervalS_;
    if (channel_.Init(addr.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel failed!";
        return -1;
    }

    return 0;
}

uint32_t AsyncRequestDone::GetRpcRetryIntervalUs(uint32_t retryCount) {
    if (retryCount == 0) {
        return rpcRetryIntervalUs_;
    }

    // EHOSTDOWN: 找不到可用的server。
    // server可能停止服务了，也可能正在退出中(返回了ELOGOFF)
    if (cntl_.ErrorCode() == EHOSTDOWN) {
        return rpcHostDownRetryIntervalUs_;
    }

    uint64_t tempRetryInteval = (uint64_t)rpcRetryIntervalUs_ * retryCount;
    return tempRetryInteval > rpcRetryMaxIntervalUs_ ? rpcRetryMaxIntervalUs_
                                                    : tempRetryInteval;
}

void ReadDone::Run() {
    std::unique_ptr<ReadDone> self_guard(this);

    if (cntl_.Failed()) {
        LOG_IF(WARNING, aioctx_->retryCount % aioRpcFailLogInterval_ == 0)
                     << "read rpc fail, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length
                     << ", cntl errorCode: " << cntl_.ErrorCode()
                     << ", cntl error: " << cntl_.ErrorText()
                     << ", retryCount = " << aioctx_->retryCount;
        aioctx_->retryCount++;
        bthread_usleep(GetRpcRetryIntervalUs(aioctx_->retryCount));
        fileClient.AioRead(fd_, aioctx_);
    } else {
        if (nebd::client::RetCode::kOK == response_.retcode()) {
            DVLOG(6) << "read success, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length;
            memcpy(aioctx_->buf,
                cntl_.response_attachment().to_string().c_str(),
                cntl_.response_attachment().size());
            aioctx_->ret = 0;
            aioctx_->cb(aioctx_);
        } else {
            LOG(ERROR) << "read fail, fd = " << fd_
                       << ", offset = " << aioctx_->offset
                       << ", length = " << aioctx_->length
                       << ", retCode = " << response_.retcode();
            aioctx_->ret = -1;
            aioctx_->cb(aioctx_);
        }
    }
}

void WriteDone::Run() {
    std::unique_ptr<WriteDone> self_guard(this);

    if (cntl_.Failed()) {
        LOG_IF(WARNING, aioctx_->retryCount % aioRpcFailLogInterval_ == 0)
                     << "write rpc fail, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length
                     << ", cntl errorCode: " << cntl_.ErrorCode()
                     << ", cntl error: " << cntl_.ErrorText()
                     << ", retryCount = " << aioctx_->retryCount;
        aioctx_->retryCount++;
        bthread_usleep(GetRpcRetryIntervalUs(aioctx_->retryCount));
        fileClient.AioWrite(fd_, aioctx_);
    } else {
        if (nebd::client::RetCode::kOK == response_.retcode()) {
            DVLOG(6) << "write success, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length;
            aioctx_->ret = 0;
            aioctx_->cb(aioctx_);
        } else {
            LOG(ERROR) << "write fail, fd = " << fd_
                       << ", offset = " << aioctx_->offset
                       << ", length = " << aioctx_->length
                       << ", retCode = " << response_.retcode();
            aioctx_->ret = -1;
            aioctx_->cb(aioctx_);
        }
    }
}

void DiscardDone::Run() {
    std::unique_ptr<DiscardDone> self_guard(this);

    if (cntl_.Failed()) {
        LOG_IF(WARNING, aioctx_->retryCount % aioRpcFailLogInterval_ == 0)
                     << "discard rpc fail, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length
                     << ", cntl errorCode: " << cntl_.ErrorCode()
                     << ", cntl error: " << cntl_.ErrorText()
                     << ", retryCount = " << aioctx_->retryCount;
        aioctx_->retryCount++;
        bthread_usleep(GetRpcRetryIntervalUs(aioctx_->retryCount));
        fileClient.Discard(fd_, aioctx_);
    } else {
        if (nebd::client::RetCode::kOK == response_.retcode()) {
            DVLOG(6) << "discard success, fd = " << fd_
                     << ", offset = " << aioctx_->offset
                     << ", length = " << aioctx_->length;
            aioctx_->ret = 0;
            aioctx_->cb(aioctx_);
        } else {
            LOG(ERROR) << "discard fail, fd = " << fd_
                       << ", offset = " << aioctx_->offset
                       << ", length = " << aioctx_->length
                       << ", retCode = " << response_.retcode();
            aioctx_->ret = -1;
            aioctx_->cb(aioctx_);
        }
    }
}

void FlushDone::Run() {
    std::unique_ptr<FlushDone> self_guard(this);

    if (cntl_.Failed()) {
        LOG_IF(WARNING, aioctx_->retryCount % aioRpcFailLogInterval_ == 0)
                     << "flush rpc fail, fd = " << fd_
                     << ", cntl errorCode: " << cntl_.ErrorCode()
                     << ", cntl error: " << cntl_.ErrorText()
                     << ", retryCount = " << aioctx_->retryCount;
        aioctx_->retryCount++;
        bthread_usleep(GetRpcRetryIntervalUs(aioctx_->retryCount));
        fileClient.Flush(fd_, aioctx_);
    } else {
        if (nebd::client::RetCode::kOK == response_.retcode()) {
            DVLOG(6) << "flush success, fd = " << fd_;
            aioctx_->ret = 0;
            aioctx_->cb(aioctx_);
        } else {
            LOG(ERROR) << "flush fail, fd = " << fd_
                       << ", retCode = " << response_.retcode();
            aioctx_->ret = -1;
            aioctx_->cb(aioctx_);
        }
    }
}
}  // namespace client
}  // namespace nebd
