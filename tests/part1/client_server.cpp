/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include <butil/logging.h>
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <json/json.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include "tests/part1/fake_client_service.h"

DEFINE_string(uuid, "12345678-1234-1234-1234-123456789012", "uuid");
DEFINE_string(port, "6667", "port");

namespace nebd {
namespace client {
int client_main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);

    // add service
    brpc::Server server;
    ClientService clientService;
    int ret = server.AddService(&clientService,
                          brpc::SERVER_DOESNT_OWN_SERVICE);
    if (ret != 0) {
        LOG(FATAL) << "add clientService error";
        return -1;
    }

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    std::string listenAddr = "127.0.0.1:" + FLAGS_port;
    ret = server.Start(listenAddr.c_str(), &option);
    if (ret != 0) {
        LOG(FATAL) << "start brpc server error";
        return -1;
    }

    // 写meta
    std::string filePath = "file_" + FLAGS_uuid;
    Json::Value root;
    root["port"] = FLAGS_port;
    int fd = open(filePath.c_str(), O_RDWR | O_CREAT | O_SYNC, 0644);
    if (fd < 0) {
        LOG(ERROR) << "open metadata fail, filePath = " << filePath
                   << ", errno = " << errno;
        return -1;
    }
    ret = write(fd, root.toStyledString().c_str(),
                            root.toStyledString().size());
    if (ret < root.toStyledString().size()) {
        LOG(ERROR) << "write matadata fail, filePath = " << filePath;
        close(fd);
        return -1;
    }
    close(fd);
    LOG(INFO) << "write metadata filePath = " << filePath
            << ", content : " << root.toStyledString().c_str();

    server.RunUntilAskedToQuit();
    return 0;
}
}  // namespace client
}  // namespace nebd

int main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    // 使进程为守护进程
    if (daemon(1, 0) < 0) {
        LOG(ERROR) << "create daemon error.";
        return -1;
    }

    //初始化日志模块
    logging::LoggingSettings log;
    log.logging_dest = logging::LOG_TO_FILE;
    log.log_file = "tests/part1/client_server.log";
    logging::InitLogging(log);

    LOG(INFO) << "start client server.";
    return nebd::client::client_main(argc, argv);
}
