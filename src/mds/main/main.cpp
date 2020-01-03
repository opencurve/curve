/*
 * Project: curve
 * Created Date: Friday October 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "src/mds/server/mds.h"

DEFINE_string(confPath, "conf/mds.conf", "mds confPath");

int main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    std::string confPath = FLAGS_confPath.c_str();

    // 加载配置
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;

    // 设置日志存放文件夹
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("mds.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no mds.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }

    // 初始化日志模块
    google::InitGoogleLogging(argv[0]);

    curve::mds::MDS mds;
    mds.Init(conf);

    // 启动mds
    mds.Run();

    // 停止server和后台线程
    mds.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}


