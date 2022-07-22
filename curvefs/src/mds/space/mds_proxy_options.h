/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday Jul 14 20:24:41 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_MDS_PROXY_OPTIONS_H_
#define CURVEFS_SRC_MDS_SPACE_MDS_PROXY_OPTIONS_H_

#include <string>
#include <vector>

#include "src/client/config_info.h"

namespace curvefs {
namespace mds {
namespace space {

struct MdsProxyOptions {
    curve::client::MetaServerOption option;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_MDS_PROXY_OPTIONS_H_
