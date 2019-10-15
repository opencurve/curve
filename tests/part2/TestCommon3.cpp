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
#include "src/part2/common_type.h"
#include "tests/part2/test_common.h"

TEST(LockFile, LockFile_1) {
    const char* file = "/111/222/333";
    NebdServerMocker mock;
    // EXPECT_CALL(mock, open(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, LockFile(file));
}

TEST(LockFile, LockFile_2) {
    const char* file = "/tmp/unit_test2";

    EXPECT_EQ(3, LockFile(file));
}

TEST(GeneratePort, GeneratePort_1) {
    ::system("rm -rf /etc/nebd");
    ::system("rm -rf  /tmp/nebd-test");
    NebdServerMocker mock;
    std::string metadata_file = "/111/222.txt";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    EXPECT_EQ(-1, GeneratePort(2));
}

TEST(GeneratePort, GeneratePort_2) {
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    EXPECT_EQ(0, GeneratePort(2));
}
/*
TEST(GetUuidFile, GetUuidFile_1)
{
    g_filePath = "/tmp";
    g_uuid = const_cast<char*>("666");

    std::string uuid_file = "/tmp/666";
    EXPECT_STREQ(uuid_file.c_str(), GetUuidFile().c_str());
}

TEST(SetCpuAffinity, SetCpuAffinity_1)
{
    EXPECT_EQ(-1, SetCpuAffinity(-1));
}

TEST(SetCpuAffinity, SetCpuAffinity_2)
{
    EXPECT_EQ(0, SetCpuAffinity(1));
}
*/
TEST(GenerateFd, GenerateFd_3) {
    LOG(ERROR) << "whs start.";
    ::system("rm /tmp/unit_test");
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;
    const char* filename = "unittest";
    try {
        ptree item1;
        item1.put("filename", filename);
        item1.put("fd", 1);
        items.push_back(std::make_pair("", item1));

        root.put_child("volumes", items);

        boost::property_tree::write_json(metadata_file, root);
    } catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    std::string filename_input = "unittestnew";
    EXPECT_EQ(0, GenerateFd(const_cast<char*>(filename_input.c_str()), 2));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
