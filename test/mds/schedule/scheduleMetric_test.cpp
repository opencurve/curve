/*
 * Project: curve
 * Created Date: 20190704
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include "src/mds/schedule/schedulerMetric.h"

namespace curve {
namespace mds {
namespace schedule {
TEST(TestScheduleMetric, test_scheduler_metric) {
    auto scheduleMetric = new SchedulerMetric;

    scheduleMetric->operatorNum << 8;
    ASSERT_EQ(8, scheduleMetric->operatorNum.get_value());
    scheduleMetric->operatorNum << -4;
    ASSERT_EQ(4, scheduleMetric->operatorNum.get_value());

    scheduleMetric->addOpNum << 2;
    ASSERT_EQ(2, scheduleMetric->addOpNum.get_value());
    scheduleMetric->addOpNum << -1;
    ASSERT_EQ(1, scheduleMetric->addOpNum.get_value());

    scheduleMetric->removeOpNum << 2;
    ASSERT_EQ(2, scheduleMetric->removeOpNum.get_value());
    scheduleMetric->removeOpNum << -2;
    ASSERT_EQ(0, scheduleMetric->removeOpNum.get_value());

    scheduleMetric->transferOpNum << 2;
    ASSERT_EQ(2, scheduleMetric->transferOpNum.get_value());
    scheduleMetric->transferOpNum << -1;
    ASSERT_EQ(1, scheduleMetric->transferOpNum.get_value());

    scheduleMetric->normalOpNum << 9;
    ASSERT_EQ(9, scheduleMetric->normalOpNum.get_value());
    scheduleMetric->normalOpNum << -1;
    ASSERT_EQ(8, scheduleMetric->normalOpNum.get_value());

    scheduleMetric->highOpNum << 3;
    ASSERT_EQ(3, scheduleMetric->highOpNum.get_value());
    scheduleMetric->highOpNum << -2;
    ASSERT_EQ(1, scheduleMetric->highOpNum.get_value());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

