/*
 * Project: curve
 * Created Date: Monday April 27th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
