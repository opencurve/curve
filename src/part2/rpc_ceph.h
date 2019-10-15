/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_RPC_CEPH_H_
#define SRC_PART2_RPC_CEPH_H_

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <rbd/librbd.h>
#include <rados/librados.h>
#include "src/common/client.pb.h"
#include "src/part2/rpc_request.h"

void RbdFinishAioWrite(rbd_completion_t c, AsyncWrite* write);
void RbdFinishAioRead(rbd_completion_t c, AsyncRead* read);
void RbdFinishAioDiscard(rbd_completion_t c, AsyncDiscard* discard);
void RbdFinishAioFlush(rbd_completion_t c, AsyncFlush* flush);

/**
 * @brief rpc请求对应的ceph处理类
 * @detail
 * - Filename中包含了集群mon ip，逻辑池以及卷信息
 * - OpenFile函数根据Filename打开卷，然后就可以对卷进行读写等一系列操作了
 */
class RpcRequestCeph : public RpcRequest {
 public:
    RpcRequestCeph() {}
    virtual ~RpcRequestCeph() {}
    virtual int OpenFile(char* filename);

    virtual int Write(AsyncWrite* writejob);

    virtual int Read(AsyncRead* writejob);

    virtual int StatFile(int fd, nebd::client::StatFileResponse* response);

    virtual int GetInfo(int fd, nebd::client::GetInfoResponse* response);

    virtual int CloseFile(int fd);

    virtual int Flush(AsyncFlush* flushjob);

    virtual int Discard(AsyncDiscard* discardjob);

    virtual int Resize(int fd, uint64_t size);
    virtual int InvalidateCache(int fd);
};

#endif  // SRC_PART2_RPC_CEPH_H_
