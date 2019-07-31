/*
 * Project: curve
 * Created Date: 2019-07-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */
#include "src/tools/status_tool.h"

DEFINE_string(confPath, "conf/mds.conf", "mds confPath");

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);

    curve::tool::StatusTool statusTool;
    if (argc < 2) {
        statusTool.PrintHelp();
        return 0;
    }

    // 路径应该来自puppet配置的文件路径
    std::string confPath = FLAGS_confPath.c_str();
    Configuration conf;
    conf.SetConfigPath(confPath);
    LOG_IF(FATAL, !conf.LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;
    statusTool.InitMdsRepo(&conf, std::make_shared<curve::mds::MdsRepo>());
    return statusTool.RunCommand(argv[1]);
}
