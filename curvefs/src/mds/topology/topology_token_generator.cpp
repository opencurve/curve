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
 * Created Date: Wed Aug 22 2018
 * Author: wanghai01
 */

#include <cstdlib>
#include <ctime>
#include <string>

#include "curvefs/src/mds/topology/topology_token_generator.h"

namespace curvefs {
namespace mds {
namespace topology {

// generate a token consists of 8 lower case letters
std::string DefaultTokenGenerator::GenToken() {
    std::string ret = "";
    for (int i = 0; i < 8; i++) {
        ret.push_back('a' + std::rand() % 26);
    }
    return ret;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
