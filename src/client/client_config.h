/*
 * Project: curve
 * File Created: Tuesday, 23rd October 2018 4:46:29 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_CLIENT_CONFIG_H
#define CURVE_CLIENT_CONFIG_H

#include <glog/logging.h>
#include <string>
#include "src/common/configuration.h"
#include "src/client/config_info.h"

namespace curve {
namespace client {
// TODO(tongguangxun) :与curve系统整体配置模块统一
class ClientConfig {
 public:
    int Init(const char* configpath);

    FileServiceOption_t     GetFileServiceOption();
 private:
    FileServiceOption_t      fileServiceOption_;
    common::Configuration    conf_;
};
}   // namespace client
}   // namespace curve

#endif  // !CURVE_CLIENT_CONFIG_H
