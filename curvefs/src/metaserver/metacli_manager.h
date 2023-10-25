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
 * Date: Fri Jun 30 10:56:51 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_METACLI_MANAGER_H_
#define CURVEFS_SRC_METASERVER_METACLI_MANAGER_H_

#include <map>
#include <memory>
#include <utility>

#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::ChannelManager;
using curvefs::client::rpcclient::Cli2Client;
using curvefs::client::rpcclient::MdsClient;
using curvefs::client::rpcclient::MetaCache;
using curvefs::client::rpcclient::MetaServerClient;

struct MetaCliManagerOpt {
    MetaCacheOpt metaCacheOpt;
    ExcutorOpt executorOpt;
    ExcutorOpt internalOpt;

    std::shared_ptr<Cli2Client> cli2Cli;
    std::shared_ptr<MdsClient> mdsCli;
    std::shared_ptr<ChannelManager<MetaserverID>> channelManager;
};

class MetaCliManager {
 public:
    static MetaCliManager &GetInstance() {
        static MetaCliManager instance_;
        return instance_;
    }

    void Init(MetaCliManagerOpt &&Opt) {
        opt_ = std::move(Opt);
    }

    std::shared_ptr<MetaServerClient> GetMetaCli(uint32_t fsId);

 private:
    std::mutex mtx_;
    std::map<uint32_t, std::shared_ptr<MetaServerClient>> metaCliMap_;

    MetaCliManagerOpt opt_;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_METACLI_MANAGER_H_
