/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Fri Jun 30 10:55:44 CST 2023
 * Author: lixiaocui
 */

#include <memory>

#include "curvefs/src/metaserver/metacli_manager.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MetaServerClientImpl;

std::shared_ptr<MetaServerClient> MetaCliManager::GetMetaCli(uint32_t fsId) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto out = metaCliMap_.find(fsId);

    if (out == metaCliMap_.end()) {
        auto metaCli = std::make_shared<MetaServerClientImpl>();
        auto metaCache = std::make_shared<MetaCache>();

        metaCache->Init(opt_.metaCacheOpt, opt_.cli2Cli, opt_.mdsCli);
        metaCli->Init(opt_.executorOpt, opt_.internalOpt, metaCache,
                      opt_.channelManager);
        metaCliMap_.insert({fsId, metaCli});

        return metaCli;
    } else {
        return out->second;
    }
}
}  // namespace metaserver
}  // namespace curvefs
