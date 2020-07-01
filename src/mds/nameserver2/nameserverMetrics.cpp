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
