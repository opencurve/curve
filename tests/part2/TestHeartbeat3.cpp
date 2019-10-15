/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <cmock.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/heartbeat.h"
#include "tests/part2/test_heartbeat3.h"

TEST(CheckProc, CheckProc_1) {
    g_oldPid = 66666;
    EXPECT_EQ(-1, CheckProc("test"));
}

TEST(CheckProc, CheckProc_2) {
    g_oldPid = 1;
    EXPECT_EQ(0, CheckProc("test"));
}

TEST(CheckProc, CheckProc_3) {
    g_oldPid = -1;
    NebdServerMocker mock;
    EXPECT_CALL(mock, CheckCmdline(::testing::_, ::testing::_, ::testing::_,
                                   ::testing::_))
        .WillRepeatedly(testing::Return(-1));
    EXPECT_EQ(-1, CheckProc("test"));
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
