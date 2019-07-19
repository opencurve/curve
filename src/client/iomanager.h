/*
 * Project: curve
 * File Created: Monday, 25th February 2019 9:52:28 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_IOMANAGER_H_
#define SRC_CLIENT_IOMANAGER_H_

#include "src/client/io_tracker.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/concurrent.h"

using curve::common::Atomic;

namespace curve {
namespace client {
class IOManager {
 public:
    IOManager() {
        id_ = idRecorder_.fetch_add(1);
    }
    virtual ~IOManager() = default;

    /**
     * 获取当前iomanager的ID信息
     */
    virtual IOManagerID ID() {
        return id_;
    }

    /**
     * 处理异步返回的response
     * @param: iotracker是当前reponse的归属
     */
    virtual void HandleAsyncIOResponse(IOTracker* iotracker) = 0;

 protected:
    // iomanager id目的是为了让底层RPC知道自己归属于哪个iomanager
    IOManagerID id_;

    // global id recorder
    static Atomic<uint64_t>   idRecorder_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_IOMANAGER_H_
