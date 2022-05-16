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
 * Created Date: Mon Jul 26 18:03:27 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_METRIC_METRIC_H_
#define CURVEFS_SRC_MDS_METRIC_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/common/types.h"

namespace curvefs {
namespace mds {

// Metric for a filesystem
// includes filesystem mount number and filesystem mountpoint lists
class FsMountMetric {
 public:
    explicit FsMountMetric(const std::string& fsname)
        : fsname_(fsname),
          count_("fs_" + fsname + "_mount_count"),
          mtx_(),
          mps_() {}

    void OnMount(const Mountpoint& mp);
    void OnUnMount(const Mountpoint& mp);

 private:
    // mountpoint metric key
    // format is fs_mount_${fsname}_${host}_${port}_${mountdir}
    std::string Key(const Mountpoint& mp);

 private:
    const std::string fsname_;

    // current number of fs mountpoints
    bvar::Adder<int64_t> count_;

    using MountPointMetric =
        std::unordered_map<std::string,
                           std::unique_ptr<bvar::Status<std::string>>>;
    // protect mps_
    Mutex mtx_;

    MountPointMetric mps_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_METRIC_METRIC_H_
