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
DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds listen addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd client");
DEFINE_string(mdsDbName, "curve_mds", "mds db name");
DEFINE_int32(sessionInterSec, 5, "mds session expired second");
DEFINE_int32(updateToRepoSec, 5, "interval of update data in mds to repo");
DEFINE_uint32(dummyPort, 6667, "dummy server port");


void LoadConfigFromCmdline(Configuration *conf) {
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("mdsAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.listen.addr", FLAGS_mdsAddr);
    }

    if (GetCommandLineFlagInfo("etcdAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.etcd.endpoint", FLAGS_etcdAddr);
    }

    if (GetCommandLineFlagInfo("mdsDbName", &info) && !info.is_default) {
        conf->SetStringValue("mds.DbName", FLAGS_mdsDbName);
    }

    if (GetCommandLineFlagInfo("sessionInterSec", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.leader.sessionInterSec", FLAGS_sessionInterSec);
    }

    if (GetCommandLineFlagInfo("updateToRepoSec", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.topology.TopologyUpdateToRepoSec", FLAGS_updateToRepoSec);
    }

    if (GetCommandLineFlagInfo("dummyPort", &info) && !info.is_default) {
        conf->SetIntValue(
            "mds.dummy.listen.port", FLAGS_dummyPort);
    }
}


int main(int argc, char **argv) {
    // 初始化配置
    google::ParseCommandLineFlags(&argc, &argv, false);
    std::string confPath = FLAGS_confPath.c_str();
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;
    LoadConfigFromCmdline(conf.get());
    conf->PrintConfig();
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("mds.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no mds.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }

    // 初始化日志模块
    google::InitGoogleLogging(argv[0]);

    curve::mds::MDS mds;

    // 初始化各个选项
    mds.InitMdsOptions(conf);

    // 启动 MDS DummyServer用于主从MDS探活及Metric导出
    mds.StartDummy();

    // Master 竞选
    mds.StartCompaginLeader();

    // 选举成功进行初始化所有模块
    mds.Init();

    // 启动对外服务
    mds.Run();

    // 停止server和后台线程
    mds.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}


