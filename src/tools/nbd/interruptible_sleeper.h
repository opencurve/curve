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

/*
 * Project: curve
 * Created Date: 2019-11-25
 * Author: lixiaocui
 */

#ifndef SRC_TOOLS_NBD_INTERRUPTIBLE_SLEEPER_H_
#define SRC_TOOLS_NBD_INTERRUPTIBLE_SLEEPER_H_

#include <chrono>  // NOLINT
#include <condition_variable>  // NOLINT
#include <memory>
#include <mutex>  // NOLINT

namespace curve {
namespace nbd {
/**
 * InterruptibleSleeper 实现可 interruptible 的 sleep 功能.
 * 正常情况下 wait_for 超时, 接收到退出信号之后, 程序会立即被唤醒,
 * 退出 while 循环, 并执行 cleanup 代码.
 */
class InterruptibleSleeper {
 public:
    /**
     * @brief wait_for 等待指定时间，如果接受到退出信号立刻返回
     * @param[in] time 指定wait时长
     * @return false-收到退出信号 true-超时后退出
     */
    template <typename R, typename P>
    bool wait_for(std::chrono::duration<R, P> const& time) {
        std::unique_lock<std::mutex> lock(m);
        return !cv.wait_for(lock, time, [&] { return terminate; });
    }

    /**
     * @brief interrupt 给当前wait发送退出信号
     */
    void interrupt() {
        std::lock_guard<std::mutex> lock(m);
        terminate = true;
        cv.notify_all();
    }

 private:
    std::condition_variable cv;
    std::mutex m;
    bool terminate = false;
};

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_INTERRUPTIBLE_SLEEPER_H_
