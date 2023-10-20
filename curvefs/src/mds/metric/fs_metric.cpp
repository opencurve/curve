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
 * Created Date: Tue Jul 27 16:41:09 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/mds/metric/fs_metric.h"

namespace curvefs {
namespace mds {

void FsMetric::OnMount(const std::string& fsname, const Mountpoint& mp) {
    std::lock_guard<Mutex> lock(mountMtx_);

    auto iter = mountMetrics_.find(fsname);
    if (iter == mountMetrics_.end()) {
        auto r = mountMetrics_.emplace(fsname, new FsMountMetric(fsname));
        iter = r.first;
    }

    iter->second->OnMount(mp);
}

void FsMetric::OnUnMount(const std::string& fsname, const Mountpoint& mp) {
    std::lock_guard<Mutex> lock(mountMtx_);

    auto iter = mountMetrics_.find(fsname);
    if (iter == mountMetrics_.end()) {
        return;
    }

    iter->second->OnUnMount(mp);
}

void FsMetric::UpdateFsUsage(const std::string& fsname,
                             const curvefs::common::FsUsageInfo& usage) {
    std::lock_guard<Mutex> lock(usageMtx_);
    auto iter = usageMetrics_.find(fsname);
    if (iter == usageMetrics_.end()) {
        auto r = usageMetrics_.emplace(fsname, new FsUsageMetric(fsname));
        if (!r.second) {
            LOG(ERROR) << "insert fs usage metric failed, fsname = " << fsname;
            return;
        }
        iter = r.first;
    }
    iter->second->UpdateUsage(usage);
}

void FsMetric::DeleteFsUsage(const std::string& fsname) {
    std::lock_guard<Mutex> lock(usageMtx_);

    auto iter = usageMetrics_.find(fsname);
    if (iter == usageMetrics_.end()) {
        return;
    }

    usageMetrics_.erase(iter);
}

}  // namespace mds
}  // namespace curvefs
