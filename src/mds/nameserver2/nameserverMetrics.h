/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 20191217
 * Author: lixiaocui
 */

#ifndef SRC_MDS_NAMESERVER2_NAMESERVERMETRICS_H_
#define SRC_MDS_NAMESERVER2_NAMESERVERMETRICS_H_

#include <bvar/bvar.h>
#include <string>

namespace curve {
namespace mds {
class NameserverCacheMetrics {
 public:
    // constructor
    NameserverCacheMetrics() :
        cacheCount(NameServerMetricsPrefix, "cache_count"),
        cacheBytes(NameServerMetricsPrefix, "cache_bytes") {}

    void UpdateAddToCacheCount();

    void UpdateRemoveFromCacheCount();

    void UpdateAddToCacheBytes(uint64_t size);

    void UpdateRemoveFromCacheBytes(uint64_t size);

 public:
    const std::string NameServerMetricsPrefix = "mds_nameserver_cache_metric";

    bvar::Adder<uint32_t> cacheCount;
    bvar::Adder<uint64_t> cacheBytes;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_NAMESERVERMETRICS_H_

