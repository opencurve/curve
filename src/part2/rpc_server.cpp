/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include "src/part2/rpc_server.h"
#include <butil/logging.h>
#include <bthread/bthread.h>
#include <dirent.h>
#include <gflags/gflags.h>
#include <rados/librados.h>
#include <rbd/librbd.h>
#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <unistd.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/common_type.h"
#include "src/part2/heartbeat.h"
#include "src/part2/rados_interface.h"
#include "src/part2/rpc_ceph.h"
std::mutex g_mutex;
void QemuClientServiceImpl::OpenFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::OpenFileRequest* request,
    nebd::client::OpenFileResponse* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received open request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side();

    response->set_retcode(nebd::client::kNoOK);

    char* filename = const_cast<char*>(request->filename().c_str());

    // 如果fd已经存在，则直接返回
    int fd;
    fd = FilenameFdExist(filename);
    if (fd > 0) {
        response->set_fd(fd);
        response->set_retcode(nebd::client::kOK);
        return;
    }

    g_mutex.lock();
    if (!opening_image.empty()) {
        std::vector<std::string>::iterator ret;
        ret = std::find(opening_image.begin(), opening_image.end(),
                        request->filename());
        if (ret != opening_image.end()) {
            LOG(WARNING) << "image is opening: " << filename;
            g_mutex.unlock();
            while (1) {
                bthread_usleep(1000000);
                g_mutex.lock();
                std::vector<std::string>::iterator ret;
                ret = std::find(opening_image.begin(), opening_image.end(),
                                request->filename());
                if (ret != opening_image.end()) {
                    LOG(WARNING) << "image is opening: " << filename;
                    g_mutex.unlock();
                } else {
                    fd = FilenameFdExist(filename);
                    LOG(WARNING)
                        << "image is opening success by others: " << filename
                        << ", " << fd;
                    g_mutex.unlock();
                    response->set_fd(fd);
                    response->set_retcode(nebd::client::kOK);
                    return;
                }
            }
        }
    }
    opening_image.push_back(filename);
    g_mutex.unlock();

    std::vector<std::string> split_firstly = split(filename, ":");
    std::string block_type = split_firstly[0];

    RpcRequest* rpc_request = NULL;
    if (block_type == "rbd") {
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized block_type: " << block_type;
        return;
    }
    fd = rpc_request->OpenFile(filename);
    LOG(WARNING) << "open file, fd is: " << fd;
    if (fd > 0) {
        response->set_fd(fd);
        response->set_retcode(nebd::client::kOK);
    }

    g_mutex.lock();
    std::vector<std::string>::iterator it = opening_image.begin();
    for (; it != opening_image.end();) {
        if (*it == request->filename()) {
            LOG(WARNING) << "image open success, delete from opening_image, "
                       << *it;
            it = opening_image.erase(it);
        } else {
            ++it;
        }
    }
    g_mutex.unlock();
}

void QemuClientServiceImpl::Write(google::protobuf::RpcController* cntl_base,
                                  const nebd::client::WriteRequest* request,
                                  nebd::client::WriteResponse* response,
                                  google::protobuf::Closure* done) {
    requestForRpcWrite++;
    requestForRpcWriteCounts++;
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received write request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << ", offset is: " << request->offset()
              << ", len is: " << request->size();

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;

    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    AsyncWrite* writejob = NULL;
    if (fd <= FD_CEPH_MAX) {
        writejob = new AsyncWrite;
        writejob->done = done;
        writejob->response = response;
        writejob->request = request;
        writejob->cntl = cntl;
        writejob->buf = cntl->request_attachment().to_string();
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    if (rpc_request->Write(writejob) < 0) {
        LOG(ERROR) << "write failed. " << fd;
        delete writejob;
        return;
    }
    done_guard.release();
}

void QemuClientServiceImpl::Read(google::protobuf::RpcController* cntl_base,
                                 const nebd::client::ReadRequest* request,
                                 nebd::client::ReadResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Read request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side();

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    AsyncRead* readjob = NULL;
    if (fd <= FD_CEPH_MAX) {
        readjob = new AsyncRead;
        readjob->done = done;
        readjob->response = response;
        readjob->request = request;
        readjob->cntl = cntl;
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    if (rpc_request->Read(readjob) < 0) {
        LOG(ERROR) << "read failed. " << fd;
        delete readjob;
        return;
    }
    done_guard.release();
}

void QemuClientServiceImpl::StatFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::StatFileRequest* request,
    nebd::client::StatFileResponse* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Size request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);
    response->set_size(0);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    if (fd <= FD_CEPH_MAX) {
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    rpc_request->StatFile(fd, response);
}

void QemuClientServiceImpl::GetInfo(google::protobuf::RpcController* cntl_base,
                                    const nebd::client::GetInfoRequest* request,
                                    nebd::client::GetInfoResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Size request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);
    response->set_objsize(0);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    if (fd <= FD_CEPH_MAX) {
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    rpc_request->GetInfo(fd, response);
    return;
}

void QemuClientServiceImpl::Flush(google::protobuf::RpcController* cntl_base,
                                  const nebd::client::FlushRequest* request,
                                  nebd::client::FlushResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Flush request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    AsyncFlush* flushjob = NULL;
    if (fd <= FD_CEPH_MAX) {
        flushjob = new AsyncFlush;
        flushjob->done = done;
        flushjob->response = response;
        flushjob->request = request;
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    if (rpc_request->Flush(flushjob) < 0) {
        LOG(ERROR) << "flush failed. " << fd;
        delete flushjob;
        return;
    }
    done_guard.release();
}

void QemuClientServiceImpl::CloseFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::CloseFileRequest* request,
    nebd::client::CloseFileResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Close request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side();

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    if (fd <= FD_CEPH_MAX) {
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }

    if (rpc_request->CloseFile(fd) == 0) {
        response->set_retcode(nebd::client::kOK);
    }
    return;
}

void QemuClientServiceImpl::Discard(google::protobuf::RpcController* cntl_base,
                                    const nebd::client::DiscardRequest* request,
                                    nebd::client::DiscardResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received Discard request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    AsyncDiscard* discardjob = NULL;
    if (fd <= FD_CEPH_MAX) {
        discardjob = new AsyncDiscard;
        discardjob->done = done;
        discardjob->response = response;
        discardjob->request = request;
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }
    if (rpc_request->Discard(discardjob) < 0) {
        LOG(ERROR) << "discard failed. " << fd;
        delete discardjob;
        return;
    }
    done_guard.release();
}

void QemuClientServiceImpl::ResizeFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::ResizeRequest* request,
    nebd::client::ResizeResponse* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received ResizeFile request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    if (fd <= FD_CEPH_MAX) {
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }

    if (rpc_request->Resize(fd, request->newsize()) == 0) {
        response->set_retcode(nebd::client::kOK);
    }
    return;
}

void QemuClientServiceImpl::InvalidateCache(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::InvalidateCacheRequest* request,
    nebd::client::InvalidateCacheResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received InvalidateCache request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " (attached=" << cntl->request_attachment() << ")";

    response->set_retcode(nebd::client::kNoOK);

    int fd = request->fd();
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist. " << fd;
        return;
    }

    RpcRequest* rpc_request = NULL;
    if (fd <= FD_CEPH_MAX) {  // for ceph
        rpc_request = request_ceph;
    } else {
        LOG(ERROR) << "not recognized fd: " << fd;
        return;
    }

    if (rpc_request->InvalidateCache(fd) >= 0) {
        response->set_retcode(nebd::client::kOK);
    }
}
