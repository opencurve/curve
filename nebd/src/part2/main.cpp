/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include <stdlib.h>
#include <unistd.h>
#include <glog/logging.h>
#include "src/part2/nebd_server.h"

#define BOOST_SPIRIT_THREADSAFE

DEFINE_string(confPath, "/etc/nebd/nebd-server.conf", "nebd server conf path");

int main(int argc, char* argv[]) {
    // 解析参数
    google::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    std::string confPath = FLAGS_confPath.c_str();

    // 启动nebd server
    auto server = std::make_shared<::nebd::server::NebdServer>();
    int initRes = server->Init(confPath);
    if (initRes < 0) {
        LOG(ERROR) <<  "init nebd server fail";
        return -1;
    }
    server->RunUntilAskedToQuit();

    // 停止nebd server
    server->Fini();

    google::ShutdownGoogleLogging();
    return 0;
}
