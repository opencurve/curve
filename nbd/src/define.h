/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/*
 * Project: curve
 * Created Date: Tuesday April 21st 2020
 * Author: yangyaokai
 */

/*
 * rbd-nbd - RBD in userspace
 *
 * Copyright (C) 2015 - 2016 Kylin Corporation
 *
 * Author: Yunchuan Wen <yunchuan.wen@kylin-cloud.com>
 *         Li Wang <li.wang@kylin-cloud.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#ifndef NBD_SRC_DEFINE_H_
#define NBD_SRC_DEFINE_H_

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

#define NBD_MAX_PATH "/sys/module/nbd/parameters/nbds_max"
#define PROCESS_NAME "curve-nbd"
#define NBD_PATH_PREFIX "/sys/block/nbd"
#define DEV_PATH_PREFIX "/dev/nbd"
#define CURVETAB_PATH "/etc/curve/curvetab"

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
    // exclusive open image or not
    bool exclusive = true;
    // 指定需要映射的nbd设备路径
    std::string devpath;
    // force unmap even if the device is mounted
    bool force_unmap = false;
    // unmap等待进程退出的重试次数
    int retry_times = 25;
    // unmap重试之间的睡眠间隔
    int sleep_ms = 200;
    // device's block size
    int block_size = 4096;
    // libnebd config file path
    std::string nebd_conf;

    /**
     * @brief Return options for map operation
     */
    std::string MapOptions() const;
};

// 用户命令类型
enum class Command {
    None,
    Connect,
    Disconnect,
    List
};

inline std::string BoolOption(const std::string& name, bool value,
                              bool* firstOpt) {
    std::string opts;

    if (value) {
        if (!*firstOpt) {
            opts += ",";
        }

        opts += name;
        *firstOpt = false;
    }

    return opts;
}

inline std::string to_string(const std::string& val) {
    return val;
}

template <typename T>
inline std::string KeyValueOption(const std::string& optName, const T& value,
                                  const T& defaultValue, bool* firstOpt) {
    using curve::nbd::to_string;
    using std::to_string;

    std::string opts;

    if (value != defaultValue) {
        if (!*firstOpt) {
            opts += ",";
        }

        opts += std::string(optName + "=") + to_string(value);
        *firstOpt = false;
    }

    return opts;
}

inline std::string NBDConfig::MapOptions() const {
    std::string opts;
    bool firstOpt = true;

    opts.append(KeyValueOption("device", devpath, {}, &firstOpt));
    opts.append(BoolOption("read-only", readonly, &firstOpt));
    opts.append(KeyValueOption("nbds_max", nbds_max, 0, &firstOpt));
    opts.append(KeyValueOption("max_part", max_part, 255, &firstOpt));
    opts.append(BoolOption("try-netlink", try_netlink, &firstOpt));
    opts.append(KeyValueOption("timeout", timeout, 3600, &firstOpt));
    opts.append(KeyValueOption("block-size", block_size, 4096, &firstOpt));
    opts.append(KeyValueOption("nebd-conf", nebd_conf, {}, &firstOpt));
    opts.append(BoolOption("no-exclusive", !exclusive, &firstOpt));

    return opts.empty() ? "defaults" : opts;
}

}  // namespace nbd
}  // namespace curve

#endif  // NBD_SRC_DEFINE_H_
