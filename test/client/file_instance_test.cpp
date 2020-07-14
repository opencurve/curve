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
 * Created Date: 19-11-15
 * Author: wuhanqing
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include "src/client/file_instance.h"

namespace curve {
namespace client {

TEST(FileInstanceTest, CommonTest) {
    UserInfo userInfo{"test", "passwd"};
    MDSClient mdsClient;

    // user info invlaid
    FileInstance fi;
    ASSERT_FALSE(fi.Initialize(
        "/test", &mdsClient, UserInfo{}, FileServiceOption{}));

    // mdsclient is nullptr
    FileInstance fi2;
    ASSERT_FALSE(fi2.Initialize(
        "/test", nullptr, userInfo, FileServiceOption{}));

    // iomanager4file init failed
    FileInstance fi3;
    FileServiceOption opts;
    opts.ioOpt.taskThreadOpt.isolationTaskQueueCapacity = 0;
    opts.ioOpt.taskThreadOpt.isolationTaskThreadPoolSize = 0;

    ASSERT_FALSE(fi3.Initialize(
        "/test", &mdsClient, userInfo, opts));

    // readonly
    FileInstance fi4;
    ASSERT_TRUE(fi4.Initialize(
        "/test", &mdsClient, userInfo, FileServiceOption{}, true));
    ASSERT_EQ(-1, fi4.Write("", 0, 0));

    fi4.UnInitialize();
}

}  // namespace client
}  // namespace curve
