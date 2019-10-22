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
#include "tests/part2/test_heartbeat2.h"

TEST(CloseQemuDetachedVolumes, CloseQemuDetachedVolumes_1) {
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, CloseQemuDetachedVolumes(1));
}

TEST(CloseQemuDetachedVolumes, CloseQemuDetachedVolumes_2) {
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;

    int fd2 = 7;
    char* filename2 = const_cast<char*>("mon_host=10.182.30.27");
    FdImage_t fd_image2;
    fd_image2.filename = filename2;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd2, &fd_image2));

    g_qemuPoolVolumes.push_back("rbd/volume02");
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(0));
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));

    EXPECT_EQ(0, CloseQemuDetachedVolumes(3));
}

TEST(CloseQemuDetachedVolumes, CloseQemuDetachedVolumes_3) {
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;

    int fd2 = 7;
    char* filename2 = const_cast<char*>("mon_host=10.182.30.27");
    FdImage_t fd_image2;
    fd_image2.filename = filename2;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd2, &fd_image2));

    g_qemuPoolVolumes.push_back("rbd/volume02");
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(0));
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, CloseQemuDetachedVolumes(0));
}

TEST(CloseQemuDetachedVolumes, CloseQemuDetachedVolumes_4) {
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;

    int fd2 = 7;
    char* filename2 = const_cast<char*>("mon_host=10.182.30.27");
    FdImage_t fd_image2;
    fd_image2.filename = filename2;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd2, &fd_image2));

    g_qemuPoolVolumes.push_back("rbd/volume02");
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(0));
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, CloseQemuDetachedVolumes(0));
}

TEST(CloseQemuDetachedVolumes, CloseQemuDetachedVolumes_5) {
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;

    int fd2 = 7;
    char* filename2 = const_cast<char*>("mon_host=10.182.30.27");
    FdImage_t fd_image2;
    fd_image2.filename = filename2;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd2, &fd_image2));

    g_qemuPoolVolumes.push_back("rbd/volume02");
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(0));
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, RmFd(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, CloseQemuDetachedVolumes(0));
}

TEST(CloseQemuDetachedVolumes,
     CloseQemuDetachedVolumes_6) {
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    FdImage_t fd_image;
    fd_image.filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, &fd_image));

    g_qemuPoolVolumes.push_back("rbd/volume01");
    NebdServerMocker mock;
    EXPECT_CALL(mock, ReadQemuXmls()).WillOnce(testing::Return(0));
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));

    EXPECT_EQ(0, CloseQemuDetachedVolumes(3));
}

TEST(CheckCmdline, CheckCmdline_1) {
    char uuid[5];
    NebdServerMocker mock;
    // EXPECT_CALL(mock, open(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, CheckCmdline(6666666, "test", uuid, 4));
}

TEST(CheckCmdline, CheckCmdline_2) {
    char uuid[5];
    NebdServerMocker mock;
    // EXPECT_CALL(mock, open(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, CheckCmdline(1, "test", uuid, 4));
}

TEST(CheckProc, CheckProc_1) {
    g_oldPid = 66666;
    EXPECT_EQ(-1, CheckProc("test"));
}

TEST(CheckProc, CheckProc_2) {
    g_oldPid = 1;
    EXPECT_EQ(0, CheckProc("test"));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
