/*
 * Project: curve
 * Created Date: 18-10-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/inflight_closure.h"

namespace curve {
namespace chunkserver {

void InflightClosure::Run() {
    // 在Run结束之后，自动析构自己，这样可以避免
    // 析构函数漏调
    std::unique_ptr<InflightClosure> selfGuard(this);

    // 确保done能够被调用，目的是保证rpc一定会返回
    brpcDone_->Run();

    // closure调用的时候减1，closure创建的什么加1
    inflightThrottle_->Decrement();
}

}  // namespace chunkserver
}  // namespace curve
