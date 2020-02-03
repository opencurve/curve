/*
 * Project: curve
 * Created Date: 2020-02-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include "src/tools/common.h"

namespace curve {
namespace tool {

void TrimMetricString(std::string* str) {
    // 去掉头部空格
    str->erase(0, str->find_first_not_of(" "));
    // 去掉尾部回车
    str->erase(str->find_last_not_of("\r\n") + 1);
    // 去掉两边双引号
    str->erase(0, str->find_first_not_of("\""));
    str->erase(str->find_last_not_of("\"") + 1);
}

}  // namespace tool
}  // namespace curve
