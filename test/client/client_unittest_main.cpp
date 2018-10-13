/*
 * Project: curve
 * File Created: Wednesday, 3rd October 2018 5:08:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */


#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <iostream>

#include "include/curve_compiler_specific.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/io_condition_varaiable.h"
#include "src/client/io_context_manager.h"
#include "src/client/context_slab.h"
#include "src/client/io_context.h"
#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/request_context.h"
#include "src/client/session.h"
#include "src/client/splitor.h"

int main(int argc, char ** argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);
    int ret = RUN_ALL_TESTS();

    return ret;
}
