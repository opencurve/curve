/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/string_splitter.h>
#include <braft/cli.h>
#include <braft/configuration.h>

#include <map>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/cli.h"

namespace curve {
namespace chunkserver {

/**
 * 用于配置变更测试的命令行工具，提供了 add peer、remove peer、transfer leader、
 * get leader 的命令行
 */

DEFINE_int32(timeout_ms,
             -1, "Timeout (in milliseconds) of the operation");
DEFINE_int32(max_retry,
             3, "Max retry times of each operation");
DEFINE_string(conf,
              "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Initial configuration of the replication group");
DEFINE_string(peer,
              "", "Id of the operating peer");
DEFINE_string(new_peers,
              "", "Peers that the group is going to consists of");
DEFINE_uint32(logic_pool_id,
              1, "logic pool id");
DEFINE_uint32(copyset_id,
              100001, "copyset id");

#define CHECK_FLAG(flagname)                                            \
    do {                                                                \
        if ((FLAGS_ ## flagname).empty()) {                             \
            LOG(ERROR) << __FUNCTION__ << " requires --" # flagname ;   \
            return -1;                                                  \
        }                                                               \
    } while (0);                                                        \


int AddPeer() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);

    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId newPeer;
    if (newPeer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer << '\'';
        return -1;
    }
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status
        st = AddPeer(FLAGS_logic_pool_id, FLAGS_copyset_id, conf, newPeer, opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to add_peer : " << st;
        return -1;
    }
    return 0;
}

int RemovePeer() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);

    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId removingPeer;
    if (removingPeer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer << '\'';
        return -1;
    }
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = RemovePeer(FLAGS_logic_pool_id,
                                  FLAGS_copyset_id,
                                  conf,
                                  removingPeer,
                                  opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to remove_peer : " << st;
        return -1;
    }
    return 0;
}

int TransferLeader() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);

    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse --conf=`" << FLAGS_conf << '\'';
        return -1;
    }
    PeerId targetPeer;
    if (targetPeer.parse(FLAGS_peer) != 0) {
        LOG(ERROR) << "Fail to parse --peer=`" << FLAGS_peer << '\'';
        return -1;
    }
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = TransferLeader(FLAGS_logic_pool_id,
                                      FLAGS_copyset_id,
                                      conf,
                                      targetPeer,
                                      opt);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to transfer_leader: " << st;
        return -1;
    }
    return 0;
}

int RunCommand(const std::string &cmd) {
    if (cmd == "add_peer") {
        return AddPeer();
    }
    if (cmd == "remove_peer") {
        return RemovePeer();
    }
    if (cmd == "transfer_leader") {
        return TransferLeader();
    }
    LOG(ERROR) << "Unknown command `" << cmd << '\'';
    return -1;
}

}  // namespace chunkserver
}  // namespace curve

int main(int argc, char *argv[]) {
    const char *proc_name = strrchr(argv[0], '/');
    if (proc_name == NULL) {
        proc_name = argv[0];
    } else {
        ++proc_name;
    }
    std::string help_str;
    butil::string_printf(
        &help_str,
        "Usage: %s [Command] [OPTIONS...]\n"
        "Command:\n"
        "  add_peer --logic_pool_id=$logic_pool_id --copyset_id=$copyset_id"
        "--peer=$adding_peer --conf=$current_conf\n"
        "  remove_peer --logic_pool_id=$logic_pool_id --copyset_id=$copyset_id"
        "--peer=$removing_peer --conf=$current_conf\n"
        "  transfer_leader --logic_pool_id=$logic_pool_id --copyset_id=$copyset_id"  //NOLINT
        "--peer=$target_leader --conf=$current_conf\n",
        proc_name);
    gflags::SetUsageMessage(help_str);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        std::cerr << help_str;
        return -1;
    }

    curve::chunkserver::RunCommand(argv[1]);
    return 0;
}
