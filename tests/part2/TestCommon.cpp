/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <cmock.h>
#include <fcntl.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/common_type.h"
#include "tests/part2/test_common.h"

TEST(ReadQemuXmls, ReadQemuXmls_1) {
    NebdServerMocker mock;

    // EXPECT_CALL(mock, InitConfig()).WillOnce(testing::ReturnNull());
    ::system("rm  /etc/nebd/nebd-server.conf");
    ::system("rm -r /etc/nebd");
    EXPECT_EQ(-1, ReadQemuXmls());
}

TEST(ReadQemuXmls, ReadQemuXmls_2) {
    NebdServerMocker mock;
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    // std::string xml_dir = "/var/run/nebd/nebd-test";
    std::string xml_dir = "/111/222";
    EXPECT_CALL(mock, ReadQemuXmlDir(::testing::_))
        .WillOnce(testing::Return(xml_dir));
    // EXPECT_CALL(mock, ReadQemuXml(::testing::_)).WillOnce(testing::Return());

    EXPECT_EQ(-1, ReadQemuXmls());
}

TEST(ReadQemuXmls, ReadQemuXmls_3) {
    NebdServerMocker mock;

    ::system("rm -rf /etc/nebd");
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string xml_dir = "/tmp/nebd-test";
    ::system("mkdir  /tmp/nebd-test");
    ::system("touch /tmp/nebd-test/test");

    boost::property_tree::ptree pt;
    pt.add("name", "nebd");
    write_xml("/tmp/nebd-test/test.xml", pt);

    EXPECT_CALL(mock, ReadQemuXmlDir(::testing::_))
        .WillOnce(testing::Return(xml_dir));
    EXPECT_CALL(mock, ReadQemuXml(::testing::_)).WillOnce(testing::Return());

    EXPECT_EQ(0, ReadQemuXmls());
}

TEST(GenerateFd, GenerateFd_1) {
    ::system("rm /tmp/unit_test");
    NebdServerMocker mock;
    std::string metadata_file = "/111/222/333";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    std::string filename = "unittestnew";
    EXPECT_EQ(-1, GenerateFd(const_cast<char *>(filename.c_str()), 2));
}
/*
TEST(GenerateFd, GenerateFd_2) {
    NebdServerMocker mock;
    ::system("rm /tmp/unit_test");
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;
    try {
        root.put_child("volumes", items);

        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    std::string filename_input = "unittestnew";
    EXPECT_EQ(0, GenerateFd(const_cast<char *>(filename_input.c_str()), 2));
}

TEST(GenerateFd, GenerateFd_3)
{
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
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    std::string filename_input = "unittestnew";
    EXPECT_EQ(0, GenerateFd(const_cast<char*>(filename_input.c_str()), 2));
}
*/

TEST(RmFd, RmFd_1) {
    NebdServerMocker mock;
    std::string metadata_file = "/111/222/333";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    EXPECT_EQ(-1, RmFd(2));
}

TEST(RmFd, RmFd_2) {
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;
    try {
        // ptree item1;
        // item1.put("filename", filename);
        // item1.put("fd", 1);
        // items.push_back(std::make_pair("", item1));

        root.put_child("port", items);

        boost::property_tree::write_json(metadata_file, root);
    } catch (boost::property_tree::ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_EQ(-1, RmFd(2));
}

/*
TEST(RmFd, RmFd_3)
{
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test2";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));

    boost::property_tree::ptree root_tmp;
    boost::property_tree::ptree items_tmp;
    try {
        ptree item1, item2;
        item1.put("filename", "file1");
        item1.put("fd", 1);
        items_tmp.push_back(std::make_pair("", item1));
        item2.put("filename", "file2");
        item2.put("fd", 2);
        items_tmp.push_back(std::make_pair("", item2));

        root_tmp.put_child("volumes", items_tmp);

        boost::property_tree::write_json(metadata_file, root_tmp);
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_EQ(-1, RmFd(3));

}

TEST(RmFd, RmFd_4)
{
    NebdServerMocker mock;
    std::string metadata_file = "/tmp/unit_test";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    g_port = 6300;

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;
    try {
        ptree item1, item2;
        item1.put("filename", "file1");
        item1.put("fd", 1);
        items.push_back(std::make_pair("", item1));
        item2.put("filename", "file2");
        item2.put("fd", 2);
        items.push_back(std::make_pair("", item2));

        root.put_child("volumes", items);

        boost::property_tree::write_json(metadata_file, root);
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }

    EXPECT_EQ(0, RmFd(1));
}
*/
/*
TEST(FindPort, FindPort_1)
{
    config_t* cfg = new config_t;
    g_filePath = "/111/222";
    EXPECT_EQ(-1, FindPort(cfg, 5));
}

TEST(FindPort, FindPort_2)
{
    config_t* cfg = new config_t;
    g_uuid = 666;
    g_filePath = "/tmp/nebd/";
    ::system("mkdir /tmp/nebd");
    std::string metadata_file = "/tmp/nebd/666";

    boost::property_tree::ptree root;
    boost::property_tree::ptree items;

    try {
        root.put("port", 10);
        boost::property_tree::write_json(metadata_file, root);
    }
    catch (ptree_error pt) {
        LOG(ERROR) << "write json throw an exception. " << pt.what();
        return;
    }


    EXPECT_EQ(-1, FindPort(cfg, 5));
}


TEST(GetUuidFile, GetUuidFile_1)
{
    g_filePath = "/tmp";
    g_uuid = const_cast<char*>("666");

    std::string uuid_file = "/tmp/666";
    EXPECT_STREQ(uuid_file.c_str(), GetUuidFile().c_str());
}
*/
TEST(SetCpuAffinity, SetCpuAffinity_1) { EXPECT_EQ(-1, SetCpuAffinity(-1)); }

TEST(SetCpuAffinity, SetCpuAffinity_2) { EXPECT_EQ(0, SetCpuAffinity(1)); }

TEST(Init, Init_1) {
    ::system("rm /etc/nebd/nebd-server.conf");
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string uuid = "666";
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_3) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string uuid = "666";
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_4) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_5) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    ::system("touch /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, Reload()).WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_6) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_))
        .Times(2)
        .WillOnce(testing::Return(0))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_7) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    ::system("touch /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "retry_counts=0;\n";
    write(fd, str.c_str(), str.size());
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, Reload()).WillOnce(testing::Return(6200));
    EXPECT_EQ(-1, Init());
}
/*
TEST(Init, Init_8)
{
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    ::system("touch /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char*>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, LockFile(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, Reload()).WillOnce(testing::Return(6200));
    EXPECT_EQ(-1, Init());
}
*/

TEST(Init, Init_8) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "retry_counts=0;\n";
    write(fd, str.c_str(), str.size());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, FindPort(::testing::_, ::testing::_))
        .WillOnce(testing::Return(6200));
    EXPECT_CALL(mock, LockFile(::testing::_))
        .Times(2)
        .WillOnce(testing::Return(0))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_9) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, FindPort(::testing::_, ::testing::_))
        .WillOnce(testing::Return(6200));
    EXPECT_CALL(mock, LockFile(::testing::_))
        .Times(3)
        .WillOnce(testing::Return(0))
        .WillOnce(testing::Return(0))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, Init());
}

TEST(Init, Init_10) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    ::system("rm /tmp/unit_test");
    ::system("rm /tmp/unit_test2");
    std::string uuid = "666";
    g_oldPid = 1;
    g_uuid = const_cast<char *>(uuid.c_str());
    NebdServerMocker mock;
    std::string lockfile = "/tmp/unit_test";
    std::string metadata_file = "/tmp/unit_test2";
    g_server.Stop(0);
    g_server.Join();
    g_server.ClearServices();
    EXPECT_CALL(mock, GetUuidFile()).WillOnce(testing::Return(metadata_file));
    EXPECT_CALL(mock, GetUuidLockfile()).WillOnce(testing::Return(lockfile));
    EXPECT_CALL(mock, CheckProc(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, GeneratePort(::testing::_)).WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, FindPort(::testing::_, ::testing::_))
        .WillOnce(testing::Return(6200));
    EXPECT_CALL(mock, LockFile(::testing::_))
        .WillRepeatedly(testing::Return(0));
    EXPECT_EQ(-1, Init());
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
