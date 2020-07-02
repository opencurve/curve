/*
 * Project: nebd
 * Created Date: 2020-03-23
 * Author: charsiu
 * Copyright (c) 2020 netease
 */

#ifndef SRC_COMMON_NEBD_VERSION_H_
#define SRC_COMMON_NEBD_VERSION_H_

#include <string>

namespace nebd {
namespace common {

std::string NebdVersion();
void ExposeNebdVersion();

}  // namespace common
}  // namespace nebd

#endif  // SRC_COMMON_NEBD_VERSION_H_
