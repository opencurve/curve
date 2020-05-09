/*
 * Project: curve
 * Created Date: Tuesday April 21st 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_TOOLS_NBD_DEFINE_H_
#define SRC_TOOLS_NBD_DEFINE_H_

#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace curve {
namespace nbd {

#define HELP_INFO 1
#define VERSION_INFO 2
#define CURVE_NBD_BLKSIZE 4096UL    // CURVE后端当前支持4096大小对齐的IO

#define NBD_MAX_PATH "/sys/module/nbd/parameters/nbds_max"
#define PROCESS_NAME "curve-nbd"
#define NBD_PATH_PREFIX "/sys/block/nbd"
#define DEV_PATH_PREFIX "/dev/nbd"

using std::cerr;

struct NBDConfig {
    // 设置系统提供的nbd设备的数量
    int nbds_max = 0;
    // 设置一个nbd设备能够支持的最大分区数量
    int max_part = 255;
    // 通过测试，nbd默认的io超时时间好像是3s，这里默认将值设置为1个小时
    int timeout = 3600;
    // 设置nbd设备是否为只读
    bool readonly = false;
    // 是否需要设置最大分区数量
    bool set_max_part = false;
    // 是否以netlink方式控制nbd内核模块
    bool try_netlink = false;
    // 需要映射的后端文件名称
    std::string imgname;
    // 指定需要映射的nbd设备路径
    std::string devpath;
};

// 用户命令类型
enum class Command {
    None,
    Connect,
    Disconnect,
    List
};

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_DEFINE_H_
