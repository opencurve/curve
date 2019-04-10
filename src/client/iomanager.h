/*
 * Project: curve
 * File Created: Monday, 25th February 2019 9:52:28 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_IOMANAGER_H_
#define SRC_CLIENT_IOMANAGER_H_

#include "src/client/io_tracker.h"

namespace curve {
namespace client {
class IOManager {
 public:
    IOManager() = default;
    virtual ~IOManager() = default;

    virtual void HandleAsyncIOResponse(IOTracker* iotracker) = 0;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_IOMANAGER_H_
