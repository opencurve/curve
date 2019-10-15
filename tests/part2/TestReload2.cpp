/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <cmock.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <rados/librados.h>
#include <rbd/librbd.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/reload.h"
#include "tests/part2/test_reload2.h"

TEST(ReloadCephVolume, ReloadCephVolume_1) {
    const char* tmp = "rbdrbdvolume03auth_supportednonemon_host=10.182.30.27";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_2) {
    const char* tmp = "rbdrbdvolume03auth_supported=nonemon_host=10.182.30.27";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_3) {
    const char* tmp =
        "rbd:rbdvolume03:auth_supported=none:mon_host=10.182.30.27:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);

    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_4) {
    const char* tmp = "rbd:rbd/volume03:auth_supported=none:mon_host";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);

    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_8) {
    const char* tmp =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);
    NebdServerMocker mock;
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::ReturnNull());
    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_9) {
    const char* tmp =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);

    NebdServerMocker mock;

    rados_t* cluster = new rados_t;
    int ret = rados_create(cluster, "admin");
    if (ret < 0) {
        delete cluster;
        return;
    }
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(cluster));
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_, 4,
                                ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, ReloadCephVolume(4, filename));
}

TEST(ReloadCephVolume, ReloadCephVolume_10) {
    const char* tmp =
        "rbd:rbd/volume03:auth_supported=none:mon_host=10.182.30.27\\:6789";
    char* filename = new char[strlen(tmp) + 1]();
    snprintf(filename, strlen(tmp) + 1, "%s", tmp);

    NebdServerMocker mock;
    rados_t* cluster = new rados_t;
    EXPECT_CALL(mock, ConnectRados(::testing::_))
        .WillOnce(testing::Return(cluster));
    EXPECT_CALL(mock, OpenImage(::testing::_, ::testing::_, ::testing::_, 4,
                                ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(0, ReloadCephVolume(4, filename));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
