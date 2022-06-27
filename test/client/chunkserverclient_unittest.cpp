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
 * File Created: 2022-06-30
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>

#include "src/client/chunkserver_client.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curve {
namespace client {



}   // namespace client
}   // namespace curve

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = RUN_ALL_TESTS();
    return ret;
}
