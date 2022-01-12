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

#include "curvefs/src/mds/metric/metric.h"

#include <glog/logging.h>

#include <sstream>
#include <utility>
#include <vector>

#include "src/common/string_util.h"

namespace curvefs {
namespace mds {

void FsMountMetric::OnMount(const std::string& mp) {
    MountPoint mountpoint = ParseMountPoint(mp);
    if (!mountpoint.valid) {
        return;
    }

    std::string key = Key(mountpoint);

    {
        std::lock_guard<Mutex> lock(mtx_);

        auto* metric = new bvar::Status<std::string>();
        metric->expose(key);

        // value format is: {"host": "1.2.3.4", "dir": "/tmp"}
        metric->set_value("{\"host\": \"%s\", \"dir\": \"%s\"}",
                          mountpoint.hostname.c_str(),
                          mountpoint.mountdir.c_str());

        mps_.emplace(std::move(key), metric);
    }

    count_ << 1;
}

void FsMountMetric::OnUnMount(const std::string& mp) {
    MountPoint mountPoint = ParseMountPoint(mp);
    if (!mountPoint.valid) {
        return;
    }

    {
        std::lock_guard<Mutex> lock(mtx_);
        mps_.erase(Key(mountPoint));
    }

    count_ << -1;
}

std::string FsMountMetric::Key(const MountPoint& mp) {
    return "fs_mount_" + fsname_ + "_" + mp.hostname + "_" + mp.mountdir;
}

FsMountMetric::MountPoint FsMountMetric::ParseMountPoint(
    const std::string& mountpoint) const {
    MountPoint mp;

    std::vector<std::string> items;
    curve::common::SplitString(mountpoint, ":", &items);

    if (items.size() != 2) {
        LOG(ERROR) << "Parse mountpoint '" << mountpoint << "' failed";
        mp.valid = false;
    } else {
        mp.hostname = std::move(items[0]);
        mp.mountdir = std::move(items[1]);
        mp.valid = true;
    }

    return mp;
}

}  // namespace mds
}  // namespace curvefs
