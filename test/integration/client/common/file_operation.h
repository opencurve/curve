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
 * File Created: Friday, 30th August 2019 1:43:35 pm
 * Author: tongguangxun
 */

#ifndef TEST_INTEGRATION_CLIENT_COMMON_FILE_OPERATION_H_
#define TEST_INTEGRATION_CLIENT_COMMON_FILE_OPERATION_H_

#include <string>
#include <vector>

namespace curve {
namespace test {
class FileCommonOperation {
 public:
  /**
   * 指定文件名，打开文件，如果没创建则先创建，返回fd
   */
    static int Open(const std::string& filename, const std::string& owner);

    static void Close(int fd);

    static int Open(const std::string& filename, const std::string& owner,
                              uint64_t stripeUnit, uint64_t stripeCount);
};
}   //  namespace test
}   //  namespace curve

#endif  // TEST_INTEGRATION_CLIENT_COMMON_FILE_OPERATION_H_
