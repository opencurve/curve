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
#include "tests/part2/test_reload.h"

TEST(Reload, reload_1) {
    NebdServerMocker mock;
    std::string metadata_file = "/var/444";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    EXPECT_EQ(-1, Reload());
}

TEST(Reload, reload_2) {
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("volume", 5);
        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_EQ(-1, Reload());
}

TEST(Reload, reload_3) {
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", 10);
        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_EQ(10, Reload());
}

TEST(Reload, reload_4) {
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    const char* filename1 = "";
    const char* filename2 = "unittest";
    try {
        root.put("port", 10);
        boost::property_tree::write_json(metadata_file, root);

        boost::property_tree::ptree item1, item2;
        item1.put("filename", filename1);
        item1.put("fd", 1);
        items.push_back(std::make_pair("", item1));
        item2.put("filename", filename2);
        item2.put("fd", 2);
        items.push_back(std::make_pair("", item2));

        root.put_child("volumes", items);

        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_CALL(mock, ReloadCephVolume(1, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, ReloadCephVolume(2, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(10, Reload());
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
