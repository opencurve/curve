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
 * Date: Tuesday Jul 19 10:06:34 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_MDS_PROXY_MANAGER_H_
#define CURVEFS_SRC_MDS_SPACE_MDS_PROXY_MANAGER_H_

#include <bthread/mutex.h>
#include <gtest/gtest_prod.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/space/mds_proxy.h"

namespace curvefs {
namespace mds {
namespace space {

// Manager of all curvebs mds proxies.
// The caller can get a proxy by mds hosts through `GetOrCreateProxy`, same
// hosts with different order will get the same one.
class MdsProxyManager {
    FRIEND_TEST(MdsProxyManagerTest, GetSameProxy);
    FRIEND_TEST(MdsProxyManagerTest, GetDifferentProxy);
    FRIEND_TEST(MdsProxyManagerTest, EmptyHosts);

 public:
    static MdsProxyManager& GetInstance() {
        static MdsProxyManager manager;
        return manager;
    }

    static void SetProxyOptions(const MdsProxyOptions& opts) {
        options_ = opts;
    }

    // Get or create an proxy
    MdsProxy* GetOrCreateProxy(std::vector<std::string> hosts);

 private:
    MdsProxyManager() = default;
    ~MdsProxyManager() = default;

    struct Slot {
        RWLock lock;
        // key: sorted ip:ports of curvebs cluster.
        // value: proxy to the cluster.
        std::map<std::vector<std::string>, std::unique_ptr<MdsProxy>> proxies;
    };

    static MdsProxy* CreateProxy(Slot* slot, std::vector<std::string>&& hosts);

    static constexpr size_t kSlot = 32;

    static MdsProxyOptions options_;

    Slot slots_[kSlot];
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_MDS_PROXY_MANAGER_H_
