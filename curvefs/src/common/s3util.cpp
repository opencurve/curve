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

std::string GenPathByObjName(const std::string &objName,
                             uint32_t objectPrefix_) {
    std::vector<std::string> objs;
    uint64_t inodeid;
    curve::common::SplitString(objName, "_", &objs);
    if (objectPrefix_ == 0) {
        return objName;
    } else if (objectPrefix_ == 1) {
        inodeid = std::stoll(objs[1]);
        return objs[0] + "/" + std::to_string(inodeid/1000/1000) + "/" +
               std::to_string(inodeid/1000) + "/" + objName;
    } else {
        inodeid = std::stoll(objs[1]);
        return objs[0] + "/" + std::to_string(inodeid%256) + "/" +
               std::to_string(inodeid/1000) + "/" + objName;
    }
}

bool ValidNameOfInode(const std::string &inode, const std::string &objName,
                      uint32_t objectPrefix) {
    std::vector<std::string> res, objs;
    if (objectPrefix == 0) {
        curve::common::SplitString(objName, "_", &res);
        return res.size() == 5 && res[1] == inode;
    } else {
        curve::common::SplitString(objName, "/", &objs);
        if (objs.size() == 4) {
            curve::common::SplitString(objs[3], "_", &res);
            return res.size() == 5 && res[1] == inode;
        }
        return false;
    }
}
}  // namespace s3util
}  // namespace common
}  // namespace curvefs
