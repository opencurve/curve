/*
 * Project: curve
 * Created Date: 20190704
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#include "src/mds/schedule/schedulerMetric.h"

namespace curve {
namespace mds {
namespace schedule {
SchedulerMetric* SchedulerMetric::self_ = nullptr;

SchedulerMetric* SchedulerMetric::GetInstance() {
    // scheduler metric在mds启动时初始化创建
    if (self_ == nullptr) {
        self_ = new SchedulerMetric();
    }

    return self_;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve

