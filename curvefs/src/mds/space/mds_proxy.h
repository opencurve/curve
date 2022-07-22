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
 * Date: Thursday Jul 14 19:56:50 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_MDS_PROXY_H_
#define CURVEFS_SRC_MDS_SPACE_MDS_PROXY_H_

#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/mds/space/mds_proxy_options.h"

namespace curve {
namespace client {
class MDSClient;
}  // namespace client
}  // namespace curve

namespace curvefs {

namespace common {
class Volume;
}  // namespace common

namespace mds {
namespace space {

// A proxy to curvebs cluster
class MdsProxy {
 public:
    static std::unique_ptr<MdsProxy> Create(
        const std::vector<std::string>& hosts,
        const MdsProxyOptions& opts);

    ~MdsProxy();

    bool GetVolumeInfo(const common::Volume& volume,
                       uint64_t* size,
                       uint64_t* extendAlignment);

    bool ExtendVolume(const common::Volume& volume, uint64_t size);

 private:
    MdsProxy();

    std::unique_ptr<curve::client::MDSClient> mdsClient_;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_MDS_PROXY_H_
