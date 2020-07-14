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
 * Created Date: Thu Nov 28 2019
 * Author: xuchaojie
 */

#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {

// TODO(xuchaojie): 后续放到配置文件里去
uint64_t DefaultSegmentSize = kGB * 1;
uint64_t kMiniFileLength = DefaultSegmentSize * 10;

}  // namespace mds
}  // namespace curve
