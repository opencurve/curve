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
 * Created Date: Thursday November 29th 2018
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <string>

#include "src/common/authenticator.h"

namespace curve {
namespace common {

TEST(AuthenticatorTEST, basic_test) {
    std::string key = "123456";
    std::string data = "/data/123";
    std::string sig = Authenticator::CalcString2Signature(data, key);
    std::string expect = "ZKNsnF9DXRxeb0+xTgFD2zLYkQnE6Sy/g2ebqWEAdlc=";
    ASSERT_STREQ(sig.c_str(), expect.c_str());
}

}  // namespace common
}  // namespace curve
