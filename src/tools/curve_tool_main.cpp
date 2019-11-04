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

DEFINE_string(mds_config_path, "conf/mds.conf", "mds confPath");
DEFINE_bool(example, false, "print the example of usage");

int main(int argc, char** argv) {
    std::string help_str = "Usage: curve_ops_tool [Command] [OPTIONS...]\n"
        "COMMANDS:\n"
        "space : show curve all disk type space, include total space and used space\n"  //NOLINT
        "status : show curve status, now only chunkserver status\n"
        "chunkserver-list : show curve chunkserver-list, list all chunkserver infomation\n"  //NOLINT
        "get : show the file info and the actual space of file\n"
        "list : list the file info of files in the directory\n"
        "seginfo : list the segments info of the file\n"
        "delete : delete the file, to force delete, should specify the --forcedelete=true\n"  //NOLINT
        "clean-recycle : clean the RecycleBin\n"
        "create : create file\n"
        "check-consistency : check the consistency of three copies\n"
        "add_peer : add the peer to the copyset\n"
        "remove_peer : remove the peer from the copyset\n"
        "transfer_leader : transfer the leader of the copyset to the peer\n"  //NOLINT
        "check-copyset : check the health state of copyset\n"
        "check-chunkserver : check the health state of the chunkserver\n"
        "check-server : check the health state of the server\n"
        "check-cluster : check the health state of the cluster\n";

    google::InitGoogleLogging(argv[0]);
    gflags::SetUsageMessage(help_str);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc < 2) {
        std::cout << help_str << std::endl;
        return -1;
    }

    std::string command = argv[1];
    if (command == "space" || command == "status"
                           || command == "chunkserver-list") {
        curve::tool::StatusTool statusTool;
        if (FLAGS_example) {
            statusTool.PrintHelp();
            return 0;
        }
        std::string confPath = FLAGS_mds_config_path.c_str();
        Configuration conf;
        conf.SetConfigPath(confPath);
        LOG_IF(FATAL, !conf.LoadConfig())
            << "load mds configuration fail, conf path = " << confPath;
        statusTool.InitMdsRepo(&conf, std::make_shared<curve::mds::MdsRepo>());
        return statusTool.RunCommand(command);
    } else if (command == "get" || command == "list"
                                || command == "seginfo"
                                || command == "delete"
                                || command == "clean-recycle"
                                || command == "create") {
        // 使用namespaceTool
        curve::tool::NameSpaceTool namespaceTool;
        if (FLAGS_example) {
            namespaceTool.PrintHelp(command);
            return 0;
        }
        LOG_IF(FATAL, namespaceTool.Init());
        return namespaceTool.RunCommand(command);
    } else if (command == "check-consistency") {
        // 检查三副本一致性
        CheckFileConsistency cfc;
        if (FLAGS_example) {
            cfc.PrintHelp();
            return 0;
        }
        LOG_IF(FATAL, !cfc.Init()) << "init failed!";
        LOG_IF(FATAL, !cfc.FetchFileCopyset()) << "FetchFileCopyset failed!";
        int rc = cfc.ReplicasConsistency() ? 0 : -1;
        rc == 0 ? LOG(INFO) << "consistency check success!"
                : LOG(ERROR) << "consistency check failed!";
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
        curve::tool::CopysetCheck copysetCheck;
        if (FLAGS_example) {
            copysetCheck.PrintHelp(command);
            return 0;
        }
        LOG_IF(FATAL, copysetCheck.Init());
        return copysetCheck.RunCommand(command);
    } else {
        std::cout << help_str << std::endl;
        return -1;
    }
}

