/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Mar 02 2022
 * Author: lixiaocui
 */

#include <vector>
#include "src/common/string_util.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace common {
namespace s3util {
bool ValidNameOfInode(const std::string &inode, const std::string &objName) {
    std::vector<std::string> res;
    curve::common::SplitString(objName, "_", &res);
    return res.size() == 5 && res[1] == inode;
}
}  // namespace s3util
}  // namespace common
}  // namespace curvefs
