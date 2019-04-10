/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOT_SNAPSHOT_SERVICE_H_
#define SRC_SNAPSHOT_SNAPSHOT_SERVICE_H_

#include <brpc/server.h>
#include <memory>

#include "proto/snapshotserver.pb.h"
#include "src/snapshot/snapshot_service_manager.h"

namespace curve {
namespace snapshotserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * @brief 快照转储rpc服务实现
 */
class SnapshotServiceImpl : public SnapshotService {
 public:
     /**
      * @brief 构造函数
      *
      * @param manager 快照转储服务管理对象
      */
    explicit SnapshotServiceImpl(
        std::shared_ptr<SnapshotServiceManager> manager)
        : manager_(manager) {}
    virtual ~SnapshotServiceImpl() {}

    /**
     * @brief http服务默认方法
     *
     * @param cntl rpc controller
     * @param req  http请求报文
     * @param resp http回复报文
     * @param done http异步回调闭包
     */
    void default_method(RpcController* cntl,
                        const HttpRequest* req,
                        HttpResponse* resp,
                        Closure* done);

 private:
    // 快照转储服务管理对象
    std::shared_ptr<SnapshotServiceManager> manager_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // SRC_SNAPSHOT_SNAPSHOT_SERVICE_H_
