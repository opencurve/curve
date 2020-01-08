/*
 * Project: curve
 * Created Date: 19-11-15
 * Author: wuhanqing
 * Copyright (c) 2018 netease
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
}

}  // namespace client
}  // namespace curve
