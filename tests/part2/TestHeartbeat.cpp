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
#include "tests/part2/test_heartbeat.h"

TEST(CloseDetachedVolumesProcess, CloseDetachedVolumesProcess_1) {
    ::system("rm /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_EQ(NULL, CloseDetachedVolumesProcess(NULL));
}

TEST(CloseDetachedVolumesProcess, CloseDetachedVolumesProcess_2) {
    ::system("mkdir /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, CloseQemuDetachedVolumes(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(NULL, CloseDetachedVolumesProcess(NULL));
}

TEST(HeartbeatProcess, HeartbeatProcess_1) {
    ::system("rm /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_EQ(NULL, HeartbeatProcess(NULL));
}

TEST(SkipSpace, SkipSpace_1) {
    ::system("rm /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    std::string str = "123456";
    std::string str_want = "23456";
    int i = 1;
    char* str_out = SkipSpace(const_cast<char*>(str.c_str()), &i, 8);
    EXPECT_STREQ(str_want.c_str(), str_out);
}

TEST(StopProc, StopProc_1) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, StopProc(0));
}

TEST(StopProc, StopProc_2) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, StopProc(0));
}

TEST(StopProc, StopProc_3) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));

    EXPECT_EQ(0, StopProc(0));
}

TEST(StopProc, StopProc_4) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    ::system("touch /tmp/unit_test");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));

    EXPECT_EQ(0, StopProc(0));
}

TEST(StopProc, StopProc_5) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(0, StopProc(1));
}

TEST(StopProc, StopProc_6) {
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test_lockfile");
    ::system("touch /tmp/unit_test");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    std::string lock_file = "/tmp/unit_test_lockfile";

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 6;
    char* filename = const_cast<char*>(
        "rbd:rbd/volume01:auth_supported=none:mon_host=10.182.30.27:6789");
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lock_file));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseImage(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(0, StopProc(1));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
