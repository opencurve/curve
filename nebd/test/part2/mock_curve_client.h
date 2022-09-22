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
 * Project: nebd
 * Created Date: 2020-02-05
 * Author: lixiaocui
 */

#ifndef NEBD_TEST_PART2_MOCK_CURVE_CLIENT_H_
#define NEBD_TEST_PART2_MOCK_CURVE_CLIENT_H_

#include <gmock/gmock.h>
#include <string>
#include "include/client/libcurve.h"

namespace nebd {
namespace server {
class MockCurveClient : public ::curve::client::CurveClient {
 public:
    MockCurveClient() {}
    ~MockCurveClient() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD2(Open,
                 int(const std::string&, const int));
    MOCK_METHOD2(ReOpen,
                 int(const std::string&, const int));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD2(Extend, int(const std::string&, int64_t));
    MOCK_METHOD1(StatFile, int64_t(const std::string&));
    MOCK_METHOD3(AioRead,
                 int(int, CurveAioContext*, curve::client::UserDataType));
    MOCK_METHOD3(AioWrite,
                 int(int, CurveAioContext*, curve::client::UserDataType));
    MOCK_METHOD2(AioDiscard, int(int, CurveAioContext*));
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_CURVE_CLIENT_H_
