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

#ifndef CURVEFS_SRC_MDS_METRIC_FS_METRIC_H_
#define CURVEFS_SRC_MDS_METRIC_FS_METRIC_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/metric/metric.h"

namespace curvefs {
namespace mds {

class FsMetric {
 public:
    static FsMetric& GetInstance() {
        static FsMetric fsMetric;
        return fsMetric;
    }

    void OnMount(const std::string& fsname, const Mountpoint& mp);
    void OnUnMount(const std::string& fsname, const Mountpoint& mp);
    void SetFsUsage(const std::string& fsname, const FsUsage& usage);
    void SetCapacity(const std::string& fsname, uint64_t capacity);
    void DeleteFsUsage(const std::string& fsname);

 private:
    FsMetric() = default;
    ~FsMetric() = default;

    FsMetric(const FsMetric&) = delete;
    FsMetric& operator=(const FsMetric&) = delete;

    std::unordered_map<std::string, std::unique_ptr<FsUsageMetric>>::iterator
    GetFsnameUsageMetricIter(const std::string& fsname);

 private:
    Mutex mountMtx_;
    std::unordered_map<std::string, std::unique_ptr<FsMountMetric>>
        mountMetrics_;
    Mutex usageMtx_;
    std::unordered_map<std::string, std::unique_ptr<FsUsageMetric>>
        usageMetrics_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_METRIC_FS_METRIC_H_
