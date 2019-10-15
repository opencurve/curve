/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <dirent.h>
#include <sys/stat.h>
#include <signal.h>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <rbd/librbd.h>
#include <rados/librados.h>
#include <libconfig.h>
#include <string>
#include "src/part2/heartbeat.h"
#include "src/part2/rados_interface.h"
#include "src/part2/reload.h"
#include "src/part2/rpc_server.h"
#include "src/part2/config.h"
#include "src/common/client.pb.h"
#define BOOST_SPIRIT_THREADSAFE

int main(int argc, char* argv[]) {
    // 使进程为守护进程
    if (daemon(0, 0) < 0) {
        char buffer[128];
        const char *err = "create daemon failed.";
        snprintf(buffer, sizeof(buffer),
                "echo %s >> /var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        return -1;
    }
    // 解析参数
    if (argc != 3) {
        char buffer[128];
        const char *err = "Incorrect parameter counts.";
        snprintf(buffer, sizeof(buffer),
                "echo %s >> /var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        return -1;
    }
    if (strcmp(argv[1], "-uuid") != 0) {
        char buffer[128];
        const char *err = "uuid is not given.";
        snprintf(buffer, sizeof(buffer),
                "echo %s >> /var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        return -1;
    }
    g_uuid = argv[2];
    int ret = Init();
     if (ret < 0) {
        char buffer[128];
        const char *err = "init fialed.";
        snprintf(buffer, sizeof(buffer),
                "echo %s >> /var/log/nebd/nebd-server-initfailed.log", err);
        ::system(buffer);
        return -1;
    }
    return 0;
}
