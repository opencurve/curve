/*
 * Project: curve
 * Created Date: 2019-07-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include "src/tools/status_tool.h"
#include "src/tools/namespace_tool.h"
#include "src/tools/consistency_check.h"
#include "src/tools/curve_cli.h"
#include "src/tools/copyset_check.h"
#include "src/tools/mds_client.h"


DEFINE_bool(example, false, "print the example of usage");
DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
DEFINE_string(confPath, "/etc/curve/tools.conf", "config file path of tools");
namespace brpc {
DECLARE_int32(health_check_interval);
}

void UpdateFlagsFromConf(curve::common::Configuration* conf) {
    // 如果配置文件不存在的话不报错，以命令行为准,这是为了不强依赖配置
    // 如果配置文件存在并且没有指定命令行的话，就以配置文件为准
    if (conf->LoadConfig()) {
        google::CommandLineFlagInfo info;
        if (GetCommandLineFlagInfo("mdsAddr", &info) && info.is_default) {
            conf->GetStringValue("mdsAddr", &FLAGS_mdsAddr);
        }
        if (GetCommandLineFlagInfo("etcdAddr", &info) && info.is_default) {
            conf->GetStringValue("etcdAddr", &FLAGS_etcdAddr);
        }
        if (GetCommandLineFlagInfo("rpcTimeout", &info) && info.is_default) {
            conf->GetUInt64Value("rpcTimeout", &FLAGS_rpcTimeout);
        }
        if (GetCommandLineFlagInfo("rpcRetryTimes", &info) && info.is_default) {
            conf->GetUInt64Value("rpcRetryTimes", &FLAGS_rpcRetryTimes);
        }
    }
}

int main(int argc, char** argv) {
    std::string help_str = "Usage: curve_ops_tool [Command] [OPTIONS...]\n"
        "COMMANDS:\n"
        "space : show curve all disk type space, include total space and used space\n"  //NOLINT
        "status : show the total status of the cluster\n"
        "chunkserver-status : show the chunkserver online status\n"
        "mds-status : show the mds status\n"
        "etcd-status : show the etcd status\n"
        "chunkserver-list : show curve chunkserver-list, list all chunkserver infomation\n"  //NOLINT
        "get : show the file info and the actual space of file\n"
        "list : list the file info of files in the directory\n"
        "seginfo : list the segments info of the file\n"
        "delete : delete the file, to force delete, should specify the --forcedelete=true\n"  //NOLINT
        "clean-recycle : clean the RecycleBin\n"
        "create : create file\n"
        "chunk-location : query the location of the chunk corresponding to the offset\n"  //NOLINT
        "check-consistency : check the consistency of three copies\n"
        "add_peer : add the peer to the copyset\n"
        "remove_peer : remove the peer from the copyset\n"
        "transfer_leader : transfer the leader of the copyset to the peer\n"  //NOLINT
        "check-copyset : check the health state of copyset\n"
        "check-chunkserver : check the health state of the chunkserver\n"
        "check-server : check the health state of the server\n"
        "check-cluster : check the health state of the cluster\n\n"
        "You can specify the config path by -confPath to avoid typing too many options\n";  //NOLINT

    google::InitGoogleLogging(argv[0]);
    gflags::SetUsageMessage(help_str);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc < 2) {
        std::cout << help_str << std::endl;
        return -1;
    }

    std::string confPath = FLAGS_confPath.c_str();
    curve::common::Configuration conf;
    conf.SetConfigPath(confPath);
    UpdateFlagsFromConf(&conf);
    // 关掉健康检查，否则Not Connect to的时候重试没有意义
    brpc::FLAGS_health_check_interval = -1;
    std::string command = argv[1];
    if (command == "space" || command == "status"
                           || command == "chunkserver-list"
                           || command == "chunkserver-status"
                           || command == "mds-status"
                           || command == "etcd-status") {
        auto mdsClient = std::make_shared<curve::tool::MDSClient>();
        auto etcdClient = std::make_shared<curve::tool::EtcdClient>();
        auto nameSpaceTool =
            std::make_shared<curve::tool::NameSpaceToolCore>(mdsClient);
        auto csClient = std::make_shared<curve::tool::ChunkServerClient>();
        auto copysetCheck =
            std::make_shared<curve::tool::CopysetCheckCore>(mdsClient, csClient);  // NOLINT
        curve::tool::StatusTool statusTool(mdsClient, etcdClient,
                                           nameSpaceTool, copysetCheck);
        if (FLAGS_example) {
            statusTool.PrintHelp(command);
            return 0;
        }
        if (command != "etcd-status") {
            if (mdsClient->Init(FLAGS_mdsAddr) != 0) {
                std::cout << "Init mdsClient failed!" << std::endl;
                return -1;
            }
        }
        // 不是所有命令都需要etcd地址
        bool needEtcd = (command == "etcd-status" || command == "status");
        if (needEtcd) {
            if (etcdClient->Init(FLAGS_etcdAddr) != 0) {
                std::cout << "Init etcdClient failed!" << std::endl;
                return -1;
            }
        }
        return statusTool.RunCommand(command);
    } else if (command == "get" || command == "list"
                                || command == "seginfo"
                                || command == "delete"
                                || command == "clean-recycle"
                                || command == "create"
                                || command == "chunk-location") {
        // 使用namespaceTool
        auto client = std::make_shared<curve::tool::MDSClient>();
        auto core = std::make_shared<curve::tool::NameSpaceToolCore>(client);
        curve::tool::NameSpaceTool namespaceTool(core);
        if (FLAGS_example) {
            namespaceTool.PrintHelp(command);
            return 0;
        }
        if (client->Init(FLAGS_mdsAddr) != 0) {
            std::cout << "Init mdsClient failed!" << std::endl;
            return -1;
        }
        return namespaceTool.RunCommand(command);
    } else if (command == "check-consistency") {
        // 检查三副本一致性
        CheckFileConsistency cfc;
        if (FLAGS_example) {
            cfc.PrintHelp();
            return 0;
        }
        if (!cfc.Init()) {
            std::cout << "Init failed!" << std::endl;
            return -1;
        }
        if (!cfc.FetchFileCopyset()) {
            std::cout << "FetchFileCopyset failed!" << std::endl;
            return -1;
        }
        int rc = cfc.ReplicasConsistency() ? 0 : -1;
        if (rc == 0) {
            std::cout << "consistency check success!" << std::endl;
        } else {
            std::cout << "consistency check failed!" << std::endl;
        }
        cfc.UnInit();
        return rc;
    } else if (command == "add_peer" || command == "remove_peer"
                                     || command == "transfer_leader") {
        if (FLAGS_example) {
            curve::chunkserver::PrintHelp(command);
            return 0;
        }
        return curve::chunkserver::RunCommand(command);
    } else if (command == "check-copyset" || command == "check-chunkserver"
            || command == "check-server" || command == "check-cluster") {
        auto mdsClient = std::make_shared<curve::tool::MDSClient>();
        auto csClient = std::make_shared<curve::tool::ChunkServerClient>();
        auto core = std::make_shared<curve::tool::CopysetCheckCore>(mdsClient,
                                                                    csClient);
        curve::tool::CopysetCheck copysetCheck(core);
        if (FLAGS_example) {
            copysetCheck.PrintHelp(command);
            return 0;
        }
        if (mdsClient->Init(FLAGS_mdsAddr) != 0) {
            std::cout << "Init mdsClient failed!" << std::endl;
            return -1;
        }
        return copysetCheck.RunCommand(command);
    } else {
        std::cout << help_str << std::endl;
        return -1;
    }
}

