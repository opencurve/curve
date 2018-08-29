/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:51:20 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */


#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <iostream>

#include "src/sfs/sfsMock.h"

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}
