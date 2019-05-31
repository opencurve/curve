/*
 * Project: curve
 * Created Date: Monday May 20th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}
