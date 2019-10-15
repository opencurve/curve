/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_RELOAD_H_
#define SRC_PART2_RELOAD_H_

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>

int Reload();
/**
 * @brief 从持久化文件中加载ceph卷
 * @param fd 卷对应的fd
 * @param filename 卷对应的filename，filename包含集群mon，pool，rbd等信息
 * @return 错误码
 */
int ReloadCephVolume(int fd, char* filename);
#endif  // SRC_PART2_RELOAD_H_
