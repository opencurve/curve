/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_INFLIGHT_CLOSURE_H_
#define SRC_CHUNKSERVER_INFLIGHT_CLOSURE_H_

#include <google/protobuf/stubs/common.h>

#include <memory>

#include "src/chunkserver/inflight_throttle.h"

namespace curve {
namespace chunkserver {

/**
 * 负责调用brpc closure和inflight request计数的更新
 */
class InflightClosure : public ::google::protobuf::Closure {
 public:
    InflightClosure(std::shared_ptr<InflightThrottle> inflightThrottle,
                    ::google::protobuf::Closure *done)
        : inflightThrottle_(inflightThrottle),
          brpcDone_(done) {
        // closure创建的什么加1，closure调用的时候减1
        inflightThrottle_->Increment();
    }

    ~InflightClosure() = default;

    /**
     * Run主要完成三件事情：
     * 1. inflight request计数减一
     * 2. 调用rpc closure的Run()，保证rpc返回
     * 3. 释放自己的内存
     */
    void Run() override;

 public:
    // inflight流控
    std::shared_ptr<InflightThrottle> inflightThrottle_;
    // brpc done
    ::google::protobuf::Closure *brpcDone_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_INFLIGHT_CLOSURE_H_
