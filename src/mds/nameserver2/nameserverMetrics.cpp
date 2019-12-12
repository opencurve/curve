/*
 * Project: curve
 * Created Date: 20191217
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#include "src/mds/nameserver2/nameserverMetrics.h"

namespace curve {
namespace mds {
void NameserverCacheMetrics::UpdateAddToCacheCount() {
    cacheCount << 1;
}

void NameserverCacheMetrics::UpdateRemoveFromCacheCount() {
    cacheCount << -1;
}

void NameserverCacheMetrics::UpdateAddToCacheBytes(uint64_t size) {
    cacheBytes << size;
}

void NameserverCacheMetrics::UpdateRemoveFromCacheBytes(uint64_t size) {
    cacheBytes << (0 - size);
}

}  // namespace mds
}  // namespace curve
