/*
 * Project: curve
 * File Created: Tuesday, 23rd October 2018 4:46:29 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CLIENT_CLIENT_CONFIG_H_
#define SRC_CLIENT_CLIENT_CONFIG_H_

#include <glog/logging.h>
#include <string>
#include "src/common/configuration.h"
#include "src/client/config_info.h"

namespace curve {
namespace client {
class ClientConfig {
 public:
    int Init(const char* configpath);

    FileServiceOption_t     GetFileServiceOption();
    uint16_t                GetDummyserverStartPort();

 private:
    FileServiceOption_t      fileServiceOption_;
    common::Configuration    conf_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CLIENT_CONFIG_H_
