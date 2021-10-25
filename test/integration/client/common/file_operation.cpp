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
 * File Created: Friday, 30th August 2019 1:43:45 pm
 * Author: tongguangxun
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
    int ret = Create(filename.c_str(), &userinfo, 100*1024*1024*1024ul);
    if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::EXISTS) {
        LOG(ERROR) << "file create failed! " << ret
                   << ", filename = " << filename;
        return -1;
    }

    // 再打开文件
    int fd = ::Open(filename.c_str(), &userinfo);
    if (fd < 0 && ret != -LIBCURVE_ERROR::FILE_OCCUPIED) {
        LOG(ERROR) << "Open file failed!";
        return -1;
    }

    return fd;
}

void FileCommonOperation::Close(int fd) {
    ::Close(fd);
}

int FileCommonOperation::Open(const std::string& filename,
                              const std::string& owner,
                              uint64_t stripeUnit, uint64_t stripeCount) {
    C_UserInfo_t userinfo;
    memset(userinfo.owner, 0, 256);
    memcpy(userinfo.owner, owner.c_str(), owner.size());

    // 先创建文件
    int ret = ::Create2(filename.c_str(), &userinfo,
               100*1024*1024*1024ul, stripeUnit, stripeCount);
    if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::EXISTS) {
        LOG(ERROR) << "file create failed! " << ret
                   << ", filename = " << filename;
        return -1;
    }

    // 再打开文件
    int fd = ::Open(filename.c_str(), &userinfo);
    if (fd < 0 && ret != -LIBCURVE_ERROR::FILE_OCCUPIED) {
        LOG(ERROR) << "Open file failed!";
        return -1;
    }

    return fd;
}

}   //  namespace test
}   //  namespace curve
