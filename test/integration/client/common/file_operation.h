/*
 * Project: curve
 * File Created: Friday, 30th August 2019 1:43:35 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
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
};
}   //  namespace test
}   //  namespace curve

#endif  // TEST_INTEGRATION_CLIENT_COMMON_FILE_OPERATION_H_
