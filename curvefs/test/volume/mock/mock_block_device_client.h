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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_TEST_VOLUME_MOCK_MOCK_BLOCK_DEVICE_CLIENT_H_
#define CURVEFS_TEST_VOLUME_MOCK_MOCK_BLOCK_DEVICE_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "curvefs/src/volume/block_device_client.h"

namespace curvefs {
namespace volume {

class MockBlockDeviceClient : public BlockDeviceClient {
 public:
    MockBlockDeviceClient() {}
    ~MockBlockDeviceClient() {}

    MOCK_METHOD1(Init, bool(const BlockDeviceClientOptions& options));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD2(Open,
                 bool(const std::string& filename,
                               const std::string& owner));
    MOCK_METHOD0(Close, bool());
    MOCK_METHOD3(Stat,
                 bool(const std::string& filename,
                               const std::string& owner,
                               BlockDeviceStat* statInfo));
    MOCK_METHOD3(Read, ssize_t(char* buf, off_t offset, size_t length));
    MOCK_METHOD3(Write, ssize_t(const char* buf, off_t offset, size_t length));

    MOCK_METHOD1(Readv, ssize_t(const std::vector<ReadPart>&));
    MOCK_METHOD1(Writev, ssize_t(const std::vector<WritePart>&));
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_TEST_VOLUME_MOCK_MOCK_BLOCK_DEVICE_CLIENT_H_
