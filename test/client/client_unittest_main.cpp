/*
 * Project: curve
 * File Created: Wednesday, 3rd October 2018 5:08:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */


#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <iostream>
#include <string>

#include "include/curve_compiler_specific.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/io_condition_varaiable.h"
#include "src/client/iomanager4file.h"
#include "src/client/io_tracker.h"
#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/request_context.h"
#include "src/client/file_instance.h"
#include "src/client/splitor.h"
#include "src/client/libcurve_file.h"
#include "include/client/libcurve_qemu.h"

std::string metaserver_addr = "127.0.0.1:8000";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./client.conf";   // NOLINT
std::string config = "metaserver_addr=127.0.0.1:8000\n"   // NOLINT
"get_leader_retry=3\n"\
"request_scheduler_queue_capacity=4096\n"\
"request_scheduler_threadpool_size=2\n"\
"client_chunk_op_retry_interval_us=200000\n"\
"client_chunk_op_max_retry=3\n"\
"pre_allocate_context_num=1024\n"\
"io_split_max_size_kb=64\n"\
"enable_applied_index_read=1\n"\
"loglevel=0";
using curve::client::FileClient;

int main(int argc, char ** argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int fd =  open(configpath.c_str(), O_CREAT | O_RDWR);
    int len = write(fd, config.c_str(), config.length());
    close(fd);

    // if (Init(configpath.c_str()) != 0) {
    //        LOG(FATAL) << "Fail to init config";
    // }

    int ret = RUN_ALL_TESTS();

    unlink(configpath.c_str());

    return ret;
}
