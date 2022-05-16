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
#include <string>
#include <utility>
#include <vector>

#include "src/common/string_util.h"

namespace curvefs {
namespace mds {

void FsMountMetric::OnMount(const Mountpoint& mp) {
    std::string key = Key(mp);

    {
        std::lock_guard<Mutex> lock(mtx_);

        auto* metric = new bvar::Status<std::string>();
        metric->expose(key);

        // value format is: {"host": "1.2.3.4", "port": "1234", "dir": "/tmp"}
        metric->set_value(
            "{\"host\": \"%s\", \"port\": \"%d\", \"dir\": \"%s\"}",
            mp.hostname().c_str(), mp.port(), mp.path().c_str());

        mps_.emplace(std::move(key), metric);
    }

    count_ << 1;
}

void FsMountMetric::OnUnMount(const Mountpoint& mp) {
    {
        std::lock_guard<Mutex> lock(mtx_);
        mps_.erase(Key(mp));
    }

    count_ << -1;
}

std::string FsMountMetric::Key(const Mountpoint& mp) {
    return "fs_mount_" + fsname_ + "_" + mp.hostname() + "_" +
           std::to_string(mp.port()) + "_" + mp.path();
}

}  // namespace mds
}  // namespace curvefs
