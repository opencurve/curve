/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include "src/part2/rpc_ceph.h"
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <string>
#include <vector>
#include "src/common/client.pb.h"
#include "src/part2/rpc_request.h"
#include "src/part2/rados_interface.h"
#include "src/part2/common_type.h"

void RbdFinishAioWrite(rbd_completion_t c, AsyncWrite* write) {
    google::protobuf::Closure* done = write->done;
    brpc::ClosureGuard done_guard(done);
    uint64_t offset = write->request->offset();
    uint64_t size = write->request->size();
    int fd = write->request->fd();

    int ret = rbd_aio_get_return_value(c);
    rbd_aio_release(c);

    if (ret == 0) {
        LOG(INFO) << "write success. fd is: " << fd
                   << ", offset is: " << offset << ", size is: " << size;
        write->response->set_retcode(nebd::client::kOK);
    } else {
        LOG(ERROR) << "write failed. fd is: " << fd << ", offset is: " << offset
                   << ", size is: " << size;
    }
    requestForRpcWrite--;
    requestForCephWrite--;
    delete write;
}

void RbdFinishAioRead(rbd_completion_t c, AsyncRead* read) {
    google::protobuf::Closure* done = read->done;
    brpc::ClosureGuard done_guard(done);
    uint64_t offset = read->request->offset();
    uint64_t size = read->request->size();
    int fd = read->request->fd();

    uint64_t ret = rbd_aio_get_return_value(c);
    rbd_aio_release(c);

    if (ret > 0) {
        LOG(INFO) << "read success. fd is: " << fd << ", offset is: " << offset
                   << ", request size is: " << size
                   << ", actual size is: " << ret;
        if (ret != size) LOG(ERROR) << "read not equal.";
        read->response->set_retcode(nebd::client::kOK);
        read->cntl->response_attachment().append(read->buf, size);
    } else {
        LOG(ERROR) << "read failed. fd is: " << fd << ", offset is: " << offset
                   << ", size is: " << size;
    }

    delete read->buf;
    delete read;
}

void RbdFinishAioDiscard(rbd_completion_t c, AsyncDiscard* discard) {
    google::protobuf::Closure* done = discard->done;
    brpc::ClosureGuard done_guard(done);
    uint64_t offset = discard->request->offset();
    uint64_t size = discard->request->size();
    int fd = discard->request->fd();

    int ret = rbd_aio_get_return_value(c);
    rbd_aio_release(c);

    if (ret == 0) {
        LOG(INFO) << "discard success. fd is: " << fd
                   << ", offset is: " << offset << ", size is: " << size;
        discard->response->set_retcode(nebd::client::kOK);
    } else {
        LOG(ERROR) << "discard failed. fd is: " << fd
                   << ", offset is: " << offset << ", size is: " << size;
    }

    delete discard;
}

void RbdFinishAioFlush(rbd_completion_t c, AsyncFlush* flush) {
    google::protobuf::Closure* done = flush->done;
    brpc::ClosureGuard done_guard(done);
    int fd = flush->request->fd();

    int ret = rbd_aio_get_return_value(c);
    rbd_aio_release(c);

    if (ret == 0) {
        LOG(INFO) << "flush success. fd is: " << fd;
        flush->response->set_retcode(nebd::client::kOK);
    } else {
        LOG(ERROR) << "flush failed. fd is: " << fd;
    }

    delete flush;
}

int RpcRequestCeph::OpenFile(char* rpc_filename) {
    LOG(ERROR) << "ceph open file start. " << rpc_filename;
    char* filename = new char[strlen(rpc_filename) + 1]();
    snprintf(filename, strlen(rpc_filename) + 1, "%s", rpc_filename);

    std::vector<std::string> split_firstly = split(filename, "=");
    if (split_firstly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }

    std::vector<std::string> split_secondly = split(split_firstly[0], ":");
    if (split_secondly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }

    std::vector<std::string> split_thirdly = split(split_secondly[1], "/");
    if (split_thirdly.size() <= 1) {
        LOG(ERROR) << "filename format is incorrect.";
        delete filename;
        return -1;
    }
    char* imagename = const_cast<char*>(split_thirdly[1].c_str());
    char* poolname = const_cast<char*>(split_thirdly[0].c_str());

    std::string mon_host = GetMonHost(filename);
    if (mon_host.empty()) {
        LOG(ERROR) << "mon host is null.";
        delete filename;
        return -1;
    }

    // 如果fd已经存在，则直接返回
    int fd;
    fd = FilenameFdExist(filename);
    if (fd > 0) {
        LOG(ERROR) << "fd is already exist.";
        delete filename;
        return fd;
    }

    rados_t* cluster = NULL;
    cluster = ConnectRados(mon_host.c_str());
    if (cluster == NULL) {
        LOG(ERROR) << "connect rados failed.";
        delete filename;
        return -1;
    }

    fd = OpenImage(cluster, poolname, imagename, -1, filename);
    if (fd < 0) {
        LOG(ERROR) << "open image failed.";
        CloseRados(cluster);
        delete filename;
        return -1;
    }
    std::string uuid_lockfile = GetUuidLockfile().c_str();
    int lockfd = LockFile(uuid_lockfile.c_str());
    if (lockfd < 0) {
        LOG(ERROR) << "lock file failed.";
        CloseImage(fd);
        return -1;
    }
    int ret = GenerateFd(filename, fd);
    if (ret < 0) {
        LOG(ERROR) << "generate fd failed.";
        CloseImage(fd);
        UnlockFile(lockfd);
        return -1;
    }
    UnlockFile(lockfd);
    LOG(NOTICE) << "ceph open file end.";
    return fd;
}

int RpcRequestCeph::Write(AsyncWrite* writejob) {
    rbd_completion_t c;
    int ret;
    rbd_image_t* image;
    if (!FdExist(writejob->request->fd(), &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    uint64_t offset = writejob->request->offset();
    uint64_t size = writejob->request->size();
    ret = rbd_aio_create_completion(writejob,
                                    (rbd_callback_t)RbdFinishAioWrite, &c);
    if (ret < 0) {
        LOG(ERROR) << "create write completion failed";
        return -1;
    }

    requestForCephWrite++;
    ret = rbd_aio_write(*image, offset, size, writejob->buf.c_str(), c);
    if (ret < 0) {
        LOG(ERROR) << "write image failed";
        return -1;
    }
    return 0;
}

int RpcRequestCeph::Read(AsyncRead* readjob) {
    rbd_completion_t c;
    uint64_t offset = readjob->request->offset();
    uint64_t size = readjob->request->size();

    rbd_image_t* image;
    if (!FdExist(readjob->request->fd(), &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    char* buf = new char[size + 1]();
    buf[size] = '\0';
    readjob->buf = buf;

    int ret = rbd_aio_create_completion(
        readjob, (rbd_callback_t)RbdFinishAioRead, &c);
    if (ret < 0) {
        LOG(ERROR) << "create read completion failed";
        return -1;
    }
    ret = rbd_aio_read(*image, offset, size, buf, c);
    if (ret < 0) {
        LOG(ERROR) << "read failed.";
        delete buf;
        return -1;
    }
    return 0;
}

int RpcRequestCeph::StatFile(int fd, nebd::client::StatFileResponse* response) {
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    rbd_image_info_t info;
    int ret;
    ret = rbd_stat(*image, &info, sizeof(info));
    if (ret < 0) {
        return ret;
    }
    response->set_retcode(nebd::client::kOK);
    response->set_size(info.size);
    return 0;
}

int RpcRequestCeph::GetInfo(int fd, nebd::client::GetInfoResponse* response) {
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    rbd_image_info_t info;
    int ret;
    ret = rbd_stat(*image, &info, sizeof(info));
    if (ret < 0) {
        return ret;
    }
    response->set_retcode(nebd::client::kOK);
    response->set_objsize(info.obj_size);

    return 0;
}

int RpcRequestCeph::CloseFile(int fd) {
    std::string uuid_lockfile = GetUuidLockfile();
    int lockfd = LockFile(uuid_lockfile.c_str());
    if (lockfd < 0) {
        LOG(ERROR) << "add lock failed.";
        return -1;
    }

    int ret = CloseImage(fd);
    if (ret < 0) {
        UnlockFile(lockfd);
        LOG(ERROR) << "close image failed.";
        return -1;
    }

    ret = RmFd(fd);
    if (ret < 0) {
        UnlockFile(lockfd);
        LOG(ERROR) << "rm fd failed.";
        return -1;
    }

    UnlockFile(lockfd);

    return ret;
}

int RpcRequestCeph::Discard(AsyncDiscard* discardjob) {
    rbd_completion_t c;
    int ret;
    rbd_image_t* image;
    if (!FdExist(discardjob->request->fd(), &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    uint64_t offset = discardjob->request->offset();
    uint64_t size = discardjob->request->size();
    ret = rbd_aio_create_completion(discardjob,
                                    (rbd_callback_t)RbdFinishAioDiscard, &c);
    if (ret < 0) {
        LOG(ERROR) << "create discard completion failed.";
        return -1;
    }

    ret = rbd_aio_discard(*image, offset, size, c);
    if (ret < 0) {
        LOG(ERROR) << "aio_discard failed.";
        return -1;
    }
    return 0;
}

int RpcRequestCeph::Flush(AsyncFlush* flushjob) {
    rbd_completion_t c;
    int ret;
    rbd_image_t* image;
    if (!FdExist(flushjob->request->fd(), &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    ret = rbd_aio_create_completion(flushjob,
                                    (rbd_callback_t)RbdFinishAioFlush, &c);
    if (ret < 0) {
        LOG(ERROR) << "create flush completion failed.";
        return -1;
    }

    ret = rbd_aio_flush(*image, c);
    if (ret < 0) {
        LOG(ERROR) << "aio flush failed.";
        return -1;
    }
    return 0;
}

int RpcRequestCeph::Resize(int fd, uint64_t size) {
    int ret;
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    ret = rbd_resize(*image, size);
    if (ret < 0) {
        LOG(ERROR) << "rbd resize failed.";
    }
    return ret;
}

int RpcRequestCeph::InvalidateCache(int fd) {
    int ret;
    rbd_image_t* image;
    if (!FdExist(fd, &image)) {
        LOG(ERROR) << "fd is not exist.";
        return -1;
    }

    ret = rbd_invalidate_cache(*image);
    if (ret < 0) {
        LOG(ERROR) << "rbd invalidate cache failed.";
    }
    return ret;
}
