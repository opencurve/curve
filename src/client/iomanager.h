/*
 * Project: curve
 * File Created: Monday, 25th February 2019 9:52:28 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_IOMANAGER_H
#define CURVE_IOMANAGER_H

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

#endif  // !CURVE_IOMANAGER_H
