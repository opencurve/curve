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
 * Created Date: 20190805
 * Author: lixiaocui
 */

#ifndef  SRC_COMMON_WAIT_INTERVAL_H_
#define  SRC_COMMON_WAIT_INTERVAL_H_

#include "src/common/interruptible_sleeper.h"

namespace curve {
namespace common {
class  WaitInterval {
 public:
    /**
     * Init 初始化任务的执行间隔
     *
     * @param[in] intervalMs 执行间隔单位是ms
     */
    void Init(uint64_t intervalMs);

    /**
     * WaitForNextExcution 根据最近一次的执行时间点和周期确定需要等待多久之后再执行
     */
    void WaitForNextExcution();

    /**
     * StopWait 退出sleep等待
     */
    void StopWait();

 private:
    // 最近一次的执行时间
    uint64_t lastSend_;
    // 任务的执行周期
    uint64_t intevalMs_;

    InterruptibleSleeper sleeper_;
};

}  // namespace common
}  // namespace curve

#endif  //  SRC_COMMON_WAIT_INTERVAL_H_
