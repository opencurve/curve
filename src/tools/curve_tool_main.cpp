/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2019-07-03
 * Author: hzchenwei7
 */

#include <gflags/gflags.h>
#include "src/common/curve_version.h"
#include "src/tools/curve_tool_factory.h"

const char* kHelpStr = "Usage: curve_ops_tool [Command] [OPTIONS...]\n"
        "COMMANDS:\n"
        "space : show curve all disk type space, include total space and used space\n"  //NOLINT
        "status : show the total status of the cluster\n"
        "chunkserver-status : show the chunkserver online status\n"
        "mds-status : show the mds status\n"
        "client-status : show the client status\n"
        "client-list : list all client\n"
        "etcd-status : show the etcd status\n"
        "snapshot-clone-status : show the snapshot clone server status\n"
        "copysets-status : check the health state of all copysets\n"
        "chunkserver-list : show curve chunkserver-list, list all chunkserver information\n"  //NOLINT
        "server-list : list all server information\n"
        "logical-pool-list : list all logical pool information\n"
        "cluster-status : show cluster status\n"
        "get : show the file info and the actual space of file\n"
        "list : list the file info of files in the directory\n"
        "seginfo : list the segments info of the file\n"
        "delete : delete the file, to force delete, should specify the --forcedelete=true\n"  //NOLINT
        "clean-recycle : clean the RecycleBin\n"
        "create : create file, file length unit is GB\n"
        "chunk-location : query the location of the chunk corresponding to the offset\n"  //NOLINT
        "check-consistency : check the consistency of three copies\n"
        "remove-peer : remove the peer from the copyset\n"
        "transfer-leader : transfer the leader of the copyset to the peer\n"  //NOLINT
        "reset-peer : reset the configuration of copyset, only reset to one peer is supported\n" //NOLINT
        "do-snapshot : do snapshot of the peer of the copyset\n"
        "do-snapshot-all : do snapshot of all peers of all copysets\n"
        "check-chunkserver : check the health state of the chunkserver\n"
        "check-copyset : check the health state of one copyset\n"
        "check-server : check the health state of the server\n"
        "check-operator : check the operators\n"
        "list-may-broken-vol: list all volumes on majority offline copysets\n"
        "rapid-leader-schedule: rapid leader schedule in cluster in logicalpool\n\n"  //NOLINT
        "You can specify the config path by -confPath to avoid typing too many options\n";  //NOLINT


DEFINE_bool(example, false, "print the example of usage");
DEFINE_string(confPath, "/etc/curve/tools.conf", "config file path of tools");
namespace brpc {
DECLARE_int32(health_check_interval);
}

namespace curve {
namespace tool {
extern std::string rootUserName;
extern std::string rootUserPassword;
}  // namespace tool
}  // namespace curve

void UpdateFlagsFromConf(curve::common::Configuration* conf) {
    // 如果配置文件不存在的话不报错，以命令行为准,这是为了不强依赖配置
    // 如果配置文件存在并且没有指定命令行的话，就以配置文件为准
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("mdsAddr", &info) && info.is_default) {
        conf->GetStringValue("mdsAddr", &FLAGS_mdsAddr);
    }
    if (GetCommandLineFlagInfo("mdsDummyPort", &info) && info.is_default) {
        conf->GetStringValue("mdsDummyPort", &FLAGS_mdsDummyPort);
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
    if (GetCommandLineFlagInfo("snapshotCloneAddr", &info) &&
                                                        info.is_default) {
        conf->GetStringValue("snapshotCloneAddr", &FLAGS_snapshotCloneAddr);
    }
    if (GetCommandLineFlagInfo("snapshotCloneDummyPort", &info) &&
                                                        info.is_default) {
        conf->GetStringValue("snapshotCloneDummyPort",
                                        &FLAGS_snapshotCloneDummyPort);
    }

    if (GetCommandLineFlagInfo("userName", &info) &&
        info.is_default) {
        conf->GetStringValue("rootUserName", &FLAGS_userName);
    }

    if (GetCommandLineFlagInfo("password", &info) &&
        info.is_default) {
        conf->GetStringValue("rootUserPassword", &FLAGS_password);
    }
}

bool LoadRootUserNameAndPassword(curve::common::Configuration* conf) {
    bool rc = conf->GetStringValue("rootUserName", &curve::tool::rootUserName);
    if (!rc) {
        std::cerr << "Missing rootUserName in '" << FLAGS_confPath << "'\n";
        return false;
    }

    rc = conf->GetStringValue("rootUserPassword",
                              &curve::tool::rootUserPassword);
    if (!rc) {
        std::cerr << "Mising rootUserPassword in '" << FLAGS_confPath << "'\n";
        return false;
    }

    return true;
}

int main(int argc, char** argv) {
    google::SetUsageMessage(kHelpStr);
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (argc < 2) {
        std::cout << kHelpStr << std::endl;
        return -1;
    }
    std::string command = argv[1];
    if (command == curve::tool::kVersionCmd) {
        std::cout << curve::common::CurveVersion() << std::endl;
        return 0;
    }

    std::string confPath = FLAGS_confPath.c_str();
    curve::common::Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        return -1;
    }

    if (!LoadRootUserNameAndPassword(&conf)) {
        return -1;
    }

    UpdateFlagsFromConf(&conf);

    // 关掉健康检查，否则Not Connect to的时候重试没有意义
    brpc::FLAGS_health_check_interval = -1;
    auto curveTool = curve::tool::CurveToolFactory::GenerateCurveTool(command);
    if (!curveTool) {
        std::cout << kHelpStr << std::endl;
        return -1;
    }
    if (FLAGS_example) {
        curveTool->PrintHelp(command);
        return 0;
    }
    return curveTool->RunCommand(command);
}

