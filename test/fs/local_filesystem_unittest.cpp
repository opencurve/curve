/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}
