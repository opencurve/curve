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
#include <glog/logging.h>
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
NebdClient &nebdClient = NebdClient::GetInstance();

int NebdClient::Init(const char* confpath) {
    // TODO
    return 0;
}

void NebdClient::Uninit() {
    // TODO
    LOG(INFO) << "NebdClient uninit success.";
}

int NebdClient::Open(const char* filename) {
    // TODO
    return -1;
}

int NebdClient::Close(int fd) {
    // TODO
    return -1;
}

int NebdClient::Extend(int fd, int64_t newsize) {
    // TODO
    return -1;
}

int64_t NebdClient::StatFile(int fd) {
    // TODO
    return -1;
}

int NebdClient::Discard(int fd, NebdClientAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdClient::AioRead(int fd, NebdClientAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdClient::AioWrite(int fd, NebdClientAioContext* aioctx) {
    // TODO
    return 0;
}

int NebdClient::Flush(int fd, NebdClientAioContext* aioctx) {
    // TODO
    return 0;
}

int64_t NebdClient::GetInfo(int fd) {
    // TODO
    return -1;
}

int NebdClient::InvalidCache(int fd) {
    // TODO
    return -1;
}


}  // namespace client
}  // namespace nebd
