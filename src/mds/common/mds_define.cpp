/*
 * Project: curve
 * Created Date: Thu Nov 28 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {

// TODO(xuchaojie): 后续放到配置文件里去
uint64_t DefaultSegmentSize = kGB * 1;
uint64_t kMiniFileLength = DefaultSegmentSize * 10;

}  // namespace mds
}  // namespace curve
