/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_COMMON_H_
#define SRC_TOOLS_COMMON_H_

#include <string>

namespace curve {
namespace tool {

const int kDefaultMdsDummyPort = 6667;

/**
 *  @brief 格式化，从metric获取的string
 *         去掉string两边的双引号以及空格和回车
 *  @param[out] str 要格式化的string
 */
void TrimMetricString(std::string* str);

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COMMON_H_
