/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Tue Jul 27 17:11:44 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/mds/metric/metric.h"

// #include <gtest/gtest.h>

#include <brpc/server.h>

#include "curvefs/src/mds/metric/fs_metric.h"

namespace curvefs {
namespace mds {
namespace metric {



}
}  // namespace mds
}  // namespace curvefs

using curvefs::mds::MountPoint;
using namespace curvefs::mds;  //NOLINT

int main() {
    int r = brpc::StartDummyServerAt(23456);
    if (r != 0) {
        abort();
    }

    MountPoint mp1;
    mp1.set_host("1.2.3.4");
    mp1.set_mountdir("/tmp");

    MountPoint mp2;
    mp2.set_host("1.2.3.4");
    mp2.set_mountdir("/document");

    MountPoint mp3;
    mp3.set_host("5.6.7.8");
    mp3.set_mountdir("/document");

    FsMetric::Instance().OnMount("hello", mp1);
    FsMetric::Instance().OnMount("hello", mp2);
    FsMetric::Instance().OnMount("world", mp3);

    getchar();
}
