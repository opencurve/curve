/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Jul 19 10:06:46 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/mds_proxy_manager.h"

#include <bthread/mutex.h>

#include <algorithm>
#include <cstddef>
#include <mutex>
#include <ostream>
#include <utility>

#include "absl/hash/hash.h"
#include "curvefs/src/mds/space/utils.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace mds {
namespace space {

MdsProxyOptions MdsProxyManager::options_;

MdsProxy* MdsProxyManager::GetOrCreateProxy(std::vector<std::string> hosts) {
    if (hosts.empty()) {
        LOG(WARNING) << "Hosts is empty";
        return nullptr;
    }

    std::sort(hosts.begin(), hosts.end());

    auto& slot =
        slots_[absl::Hash<std::vector<std::string>>{}(hosts) & (kSlot - 1)];

    {
        ReadLockGuard guard(slot.lock);
        auto it = slot.proxies.find(hosts);
        if (it != slot.proxies.end()) {
            return it->second.get();
        }
    }

    {
        WriteLockGuard guard(slot.lock);
        auto it = slot.proxies.find(hosts);
        if (it != slot.proxies.end()) {
            return it->second.get();
        }

        return CreateProxy(&slot, std::move(hosts));
    }
}

MdsProxy* MdsProxyManager::CreateProxy(Slot* slot,
                                       std::vector<std::string>&& hosts) {
    auto proxy = MdsProxy::Create(hosts, options_);
    if (proxy == nullptr) {
        LOG(ERROR) << "Fail to create mds proxy, cluster: "
                   << ConcatHosts(hosts);
        return nullptr;
    }

    auto* ret = proxy.get();
    slot->proxies.emplace(std::move(hosts), std::move(proxy));
    return ret;
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
