/*
 * Project: curve
 * Created Date: 20190805
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
