/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/**
 * Project: curve
 * Date: Sun Apr 26 15:54:01 CST 2020
 * Author: wuhanqing
 */

#ifndef SRC_TOOLS_NBD_NBDWATCHCONTEXT_H_
#define SRC_TOOLS_NBD_NBDWATCHCONTEXT_H_

#include <atomic>
#include <thread>  // NOLINT
#include <memory>
#include "src/tools/nbd/ImageInstance.h"
#include "src/tools/nbd/NBDController.h"
#include "src/tools/nbd/interruptible_sleeper.h"

namespace curve {
namespace nbd {

// 定期获取卷大小
// 卷大小发生变化后，通知NBDController
class NBDWatchContext {
 public:
    NBDWatchContext(NBDControllerPtr nbdCtrl,
                    std::shared_ptr<ImageInstance> image,
                    uint64_t currentSize)
        : nbdCtrl_(nbdCtrl),
          image_(image),
          currentSize_(currentSize),
          started_(false) {}

    ~NBDWatchContext() {
        StopWatch();
    }

    /**
     * @brief 开始定期获取卷大小任务
     */
    void WatchImageSize();

    /**
     * @brief 停止任务
     */
    void StopWatch();

 private:
    void WatchFunc();

    // nbd控制器
    NBDControllerPtr nbdCtrl_;

    // 当前卷实例
    std::shared_ptr<ImageInstance> image_;

    // 当前卷大小
    uint64_t currentSize_;

    // 任务是否开始
    std::atomic<bool> started_;

    // 任务线程
    std::thread watchThread_;

    InterruptibleSleeper sleeper_;
};

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_NBDWATCHCONTEXT_H_
