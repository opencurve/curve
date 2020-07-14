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
 * Created Date: 18-10-7
 * Author: wudemiao
 */

#ifndef TEST_CLIENT_MOCK_FILE_CLIENT_H_
#define TEST_CLIENT_MOCK_FILE_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>

#include "src/client/libcurve_file.h"

namespace curve {
namespace client {

class MockFileClient : public FileClient {
 public:
    MockFileClient() : FileClient() {}
    ~MockFileClient() = default;

    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD2(Open4ReadOnly, int(const std::string&, const UserInfo_t&));
    MOCK_METHOD4(Read, int(int, char*, off_t, size_t));
    MOCK_METHOD4(Write, int(int, const char*, off_t, size_t));
    MOCK_METHOD2(AioRead, int(int, CurveAioContext*));
    MOCK_METHOD2(AioWrite, int(int, CurveAioContext*));
    MOCK_METHOD3(StatFile, int(const std::string&,
                               const UserInfo_t&,
                               FileStatInfo*));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD0(UnInit, void());
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_FILE_CLIENT_H_
