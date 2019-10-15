/*
 *** Project: nebd
 *** File Created: 2019-09-30
 *** Author: hzwuhongsong
 *** Copyright (c) 2019 NetEase
 ***/

#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <cmock.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/common_type.h"
#include "tests/part2/test_common2.h"

TEST(FindPort, FindPort_1) {
    config_t* cfg = new config_t;
    g_filePath = "/111/222";
    EXPECT_EQ(-1, FindPort(cfg, 5));
}

TEST(FindPort, FindPort_2) {
    NebdServerMocker mock;
    config_t* cfg = new config_t;
    g_uuid = const_cast<char*>("99999");
    g_filePath = "/tmp/nebd/";
    ::system("mkdir /tmp/nebd");
    ::system("touch /tmp/nebd/99999");

    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, FindPort(cfg, 6234));
}
/*
TEST(FindPort, FindPort_3)
{
    NebdServerMocker mock;
    config_t* cfg = new config_t;
    //g_uuid = const_cast<char*>("99999");
    g_filePath = "/tmp/nebd_test2/";
    ::system("rm -r /tmp/nebd_test2");
    ::system("mkdir /tmp/nebd_test2");
    ::system("touch /tmp/nebd_test2/99999");

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(1));
    //EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(1));

    //port_option_t port_option;
    //port_option.min_port = 6200;
   // port_option.max_port = 6500;
   // EXPECT_CALL(mock, read_config_port(::testing::_, ::testing::_))
     //          .WillOnce(SetArgReferee<1>(port_option))
      //         .WillOnce(testing::Return());;

    EXPECT_EQ(6234, FindPort(cfg, 6234));
}
*/
TEST(FindPort, FindPort_4) {
    NebdServerMocker mock;
    config_t* cfg = new config_t;
    g_uuid = const_cast<char*>("666");
    g_filePath = "/tmp/nebd/";
    ::system("rm -r  /tmp/nebd");
    ::system("mkdir /tmp/nebd");
    ::system("touch /tmp/nebd/666");
    std::string metadata_file = "/tmp/nebd/666";

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", 10);
        boost::property_tree::write_json(metadata_file, root);
    } catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(1000));
    // EXPECT_CALL(mock, unLockFile(1000)).WillOnce(testing::Return());
    EXPECT_EQ(6234, FindPort(cfg, 6234));
}
/*
TEST(FindPort, FindPort_5)
{
    NebdServerMocker mock;
    config_t* cfg = new config_t;
    g_uuid = const_cast<char*>("666");
    g_filePath = "/tmp/nebd/";
    ::system("rm -r  /tmp/nebd");
    ::system("mkdir /tmp/nebd");
    std::string metadata_file = "/tmp/nebd/666";

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", 6234);
        boost::property_tree::write_json(metadata_file, root);
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(1));
    EXPECT_EQ(6235, FindPort(cfg, 6234));
}

TEST(FindPort, FindPort_6)
{
    NebdServerMocker mock;
    config_t* cfg = new config_t;
    g_uuid = const_cast<char*>("666");
    g_filePath = "/tmp/nebd/";
    ::system("rm -r  /tmp/nebd");
    ::system("mkdir /tmp/nebd");
    std::string metadata_file = "/tmp/nebd/666";

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", 7000);
        boost::property_tree::write_json(metadata_file, root);
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(1));
    EXPECT_CALL(mock, unLockFile(1)).WillOnce(testing::Return());
    EXPECT_EQ(0, FindPort(cfg, 7000));
}
*/

TEST(Squeeze, Squeeze_1) {
    std::string str = "10.182.30.27\\:6789";
    Squeeze(const_cast<char*>(str.c_str()), '\\');
    std::string str_out = "10.182.30.27:6789";
    EXPECT_STREQ(str_out.c_str(), str.c_str());
}

TEST(GetMonHost, GetMonHost_1) {
    std::string str = ":test";
    std::string host;
    std::string host_actual = GetMonHost(str);
    EXPECT_STREQ(host.c_str(), host_actual.c_str());
}

TEST(GetMonHost, GetMonHost_2) {
    std::string str =
        "rbd:rbd/"
        "ci_rbd_sys_disk:auth_supported=none:mon_host=10.182.30.27\\:6789";
    std::string host = "10.182.30.27:6789";
    std::string host_actual = GetMonHost(str);
    EXPECT_STREQ(host.c_str(), host_actual.c_str());
}

TEST(GetUuidFile, GetUuidFile_1) {
    ::system("rm -r  /tmp/nebd");
    g_filePath = "/tmp";
    g_uuid = const_cast<char*>("666");

    std::string uuid_file = "/tmp/666";
    std::string uuid_actual = GetUuidFile();
    EXPECT_STREQ(uuid_file.c_str(), uuid_actual.c_str());
}
TEST(GetUuidLockfile, GetUuidLockfile_1) {
    ::system("rm -r  /tmp/nebd");
    g_uuid = const_cast<char*>("99988");

    std::string LockFile = "/tmp/99988";
    std::string lock_actual = GetUuidLockfile();
    EXPECT_STREQ(LockFile.c_str(), lock_actual.c_str());
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
