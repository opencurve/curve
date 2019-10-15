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
#include <fcntl.h>
#include <cmock.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/rados_interface.h"
#include "tests/part2/test_rados.h"

TEST(ReadRpcOption, ReadRpcOption_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    rpc_option_t option;
    ReadRpcOption(cfg, &option);
    EXPECT_EQ(2, option.brpc_num_threads);
}

TEST(ReadRpcOption, ReadRpcOption_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str =
        "brpc_num_threads=2;\nbrpc_idle_timeout=2;\nbrpc_max_concurrency=2;"
        "\nbrpc_methodmax_concurrency=2;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    rpc_option_t option;
    ReadRpcOption(cfg, &option);
    EXPECT_EQ(2, option.brpc_idle_timeout);
}

TEST(ReadPort, ReadPort_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    // NebdServerMocker mock;
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    port_option_t option;
    ReadPort(cfg, &option);
    EXPECT_EQ(6200, option.min_port);
}

TEST(ReadPort, ReadPort_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "min_port = 6203;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    port_option_t option;
    ReadPort(cfg, &option);
    EXPECT_EQ(6203, option.min_port);
}

TEST(ReadLogLevel, ReadLogLevel_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    // NebdServerMocker mock;
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(1, ReadLogLevel(cfg));
}

TEST(ReadLogLevel, ReadLogLevel_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "loglevel = 2;\n";
    write(fd, str.c_str(), str.size());

    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadLogLevel(cfg));
}

TEST(ReadUuidPath, ReadUuidPath_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    std::string path = "/var/run/nebd-server/";
    EXPECT_STREQ(path.c_str(), ReadUuidPath(cfg).c_str());
}

TEST(ReadDetachedTimes, ReadDetachedTimes_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(6, ReadDetachedTimes(cfg));
}

TEST(ReadDetachedTimes, ReadDetachedTimes_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "detached_times = 2;\n";
    write(fd, str.c_str(), str.size());

    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadDetachedTimes(cfg));
}

TEST(ReadRetryCounts, ReadRetryCounts_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(3, ReadRetryCounts(cfg));
}

TEST(ReadRetryCounts, ReadRetryCounts_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "retry_counts = 2;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadRetryCounts(cfg));
}

TEST(ReadLockPortFile, ReadLockPortFile_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }

    std::string path = "/tmp/nebd-server.port.file.lock";
    EXPECT_STREQ(path.c_str(), ReadLockPortFile(cfg).c_str());
}

TEST(ReadCephConf, ReadCephConf_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }

    std::string path = "/etc/ceph/ceph.conf";
    EXPECT_STREQ(path.c_str(), ReadCephConf(cfg).c_str());
}

TEST(ReadLogPath, ReadLogPath_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }

    std::string path = "/var/log/nebd/";
    EXPECT_STREQ(path.c_str(), ReadLogPath(cfg).c_str());
}

TEST(ReadQemuXmlDir, ReadQemuXmlDir_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }

    std::string path = "/var/run/libvirt/qemu/";
    EXPECT_STREQ(path.c_str(), ReadQemuXmlDir(cfg).c_str());
}

TEST(ReadHeartbeatInterval, ReadHeartbeatInterval_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(1, ReadHeartbeatInterval(cfg));
}

TEST(ReadHeartbeatInterval, ReadHeartbeatInterval_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "heartbeat_interval = 2;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadHeartbeatInterval(cfg));
}

TEST(ReadAssertTimes, ReadAssertTimes_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(10, ReadAssertTimes(cfg));
}

TEST(ReadAssertTimes, ReadAssertTimes_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "check_assert_times = 2;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadAssertTimes(cfg));
}

TEST(ReadCheckDetachedInterval, ReadCheckDetachedInterval_1) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(5, ReadCheckDetachedInterval(cfg));
}

TEST(ReadCheckDetachedInterval, ReadCheckDetachedInterval_2) {
    ::system("mkdir -p /etc/nebd");
    ::system("rm /etc/nebd/nebd-server.conf");
    ::system("touch /etc/nebd/nebd-server.conf");
    std::string file_path = "/etc/nebd/nebd-server.conf";
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    std::string str = "check_detached_interval = 2;\n";
    write(fd, str.c_str(), str.size());
    config_t* cfg = InitConfig();
    if (cfg == NULL) {
        LOG(ERROR) << "init config failed.";
        return;
    }
    EXPECT_EQ(2, ReadCheckDetachedInterval(cfg));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
