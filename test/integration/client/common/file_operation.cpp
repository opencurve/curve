/*
 * Project: curve
 * File Created: Friday, 30th August 2019 1:43:45 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
 */

#include <glog/logging.h>

#include <map>
#include <cmath>
#include <numeric>
#include <chrono>   //  NOLINT
#include <atomic>
#include <thread>   //  NOLINT
#include <algorithm>
#include <functional>

#include "src/common/timeutility.h"
#include "include/client/libcurve.h"
#include "src/client/inflight_controller.h"
#include "test/integration/client/common/file_operation.h"

namespace curve {
namespace test {
int FileCommonOperation::Open(const std::string& filename,
                              const std::string& owner) {
    C_UserInfo_t userinfo;
    memset(userinfo.owner, 0, 256);
    memcpy(userinfo.owner, owner.c_str(), owner.size());

    // 先创建文件
    int ret = Create(filename.c_str(), &userinfo, 10*1024*1024*1024ul);
    if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::EXISTS) {
        LOG(ERROR) << "file create failed! " << ret;
        return -1;
    }

    // 再打开文件
    int fd = ::Open(filename.c_str(), &userinfo);
    if (fd < 0) {
        LOG(ERROR) << "Open file failed!";
        return -1;
    }

    return fd;
}
}   //  namespace test
}   //  namespace curve
