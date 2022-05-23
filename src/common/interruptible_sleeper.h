/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2019-11-25
 * Author: lixiaocui
 */

#ifndef SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_
#define SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_

#include <chrono>  // NOLINT
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace common {
/**
 * InterruptibleSleeper 实现可 interruptible 的 sleep 功能.
 * 正常情况下 wait_for 超时, 接收到退出信号之后, 程序会立即被唤醒,
 * 退出 while 循环, 并执行 cleanup 代码.
 */
class InterruptibleSleeper {
 public:
    /**
     * @brief wait_for 等待指定时间，如果接受到退出信号立刻返回
     *
     * @param[in] time 指定wait时长
     *
     * @return false-收到退出信号 true-超时后退出
     */
    template<typename R, typename P>
    bool wait_for(std::chrono::duration<R, P> const& time) {
        UniqueLock lock(m);
        return !cv.wait_for(lock, time, [&]{return terminate;});
    }

    /**
     * @brief interrupt 给当前wait发送退出信号
     */
    void interrupt() {
        UniqueLock lock(m);
        terminate = true;
        cv.notify_all();
    }

    void init() {
        UniqueLock lock(m);
        terminate = false;
    }

 private:
    ConditionVariable cv;
    Mutex m;
    bool terminate = false;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_

