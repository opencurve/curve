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

#ifndef SRC_MDS_NAMESERVER2_METRIC_H_
#define SRC_MDS_NAMESERVER2_METRIC_H_

#include <bvar/bvar.h>
#include <string>

namespace curve {
namespace mds {
class NameserverCacheMetrics {
 public:
    // constructor
    NameserverCacheMetrics() :
        cacheCount(NameServerMetricsPrefix, "cache_count"),
        cacheBytes(NameServerMetricsPrefix, "cache_bytes"),
        cacheHit(NameServerMetricsPrefix, "cache_hit"),
        cacheMiss(NameServerMetricsPrefix, "cache_miss") {}

    void UpdateAddToCacheCount();

    void UpdateRemoveFromCacheCount();

    void UpdateAddToCacheBytes(uint64_t size);

    void UpdateRemoveFromCacheBytes(uint64_t size);

    void OnCacheHit() {
        cacheHit << 1;
    }

    void OnCacheMiss() {
        cacheMiss << 1;
    }

 public:
    const std::string NameServerMetricsPrefix = "mds_nameserver_cache_metric";

    bvar::Adder<uint32_t> cacheCount;
    bvar::Adder<uint64_t> cacheBytes;
    bvar::Adder<uint64_t> cacheHit;
    bvar::Adder<uint64_t> cacheMiss;
};

class SegmentDiscardMetric {
 public:
    SegmentDiscardMetric()
        : prefix_("mds_nameserver_discard"),
          totalCleanedSegments_(prefix_ + "_total_cleaned_segment_count"),
          pendingSegments_(prefix_ + "_pending_segment_count"),
          totalCleanedSize_(prefix_ + "_total_cleaned_size"),
          pendingSize_(prefix_ + "_pending_size") {}

    ~SegmentDiscardMetric() = default;

    void OnReceiveDiscardRequest(int64_t size);
    void OnDiscardFinish(int64_t size);

 public:
    const std::string prefix_;

    bvar::Adder<int64_t> totalCleanedSegments_;
    bvar::Adder<int64_t> pendingSegments_;

    bvar::Adder<int64_t> totalCleanedSize_;
    bvar::Adder<int64_t> pendingSize_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_METRIC_H_
