/*
 * Project: curve
 * Created Date: 20191217
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_NAMESERVER2_NAMESERVERMETRICS_H_
#define SRC_MDS_NAMESERVER2_NAMESERVERMETRICS_H_

#include <bvar/bvar.h>
#include <string>

namespace curve {
namespace mds {
class NameserverCacheMetrics {
 public:
    // 构造函数
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

