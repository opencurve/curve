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
 * Date: Tuesday Nov 23 11:20:08 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_

#include <braft/raft.h>

#include <utility>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/metaserver/copyset/types.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::mds::heartbeat::ConfigChangeType;
using ::curvefs::common::Peer;

struct OngoingConfChange {
    OngoingConfChange() : type(ConfigChangeType::NONE), alterPeer() {}

    OngoingConfChange(ConfigChangeType type, const Peer& peer)
        : type(type), alterPeer(peer) {}

    OngoingConfChange(ConfigChangeType type, Peer&& peer)
        : type(type), alterPeer(std::move(peer)) {}

    bool HasConfChange() const {
        return type != ConfigChangeType::NONE && alterPeer.has_address();
    }

    void Reset() {
        type = ConfigChangeType::NONE;
        alterPeer.clear_address();
    }

    ConfigChangeType type;
    Peer alterPeer;
};

class CopysetNode;

class OnConfChangeDone : public braft::Closure {
 public:
    OnConfChangeDone(CopysetNode* node, braft::Closure* done,
                     const OngoingConfChange& confChange)
        : node_(node), done_(done), confChange_(confChange) {}

    void Run() override;

 private:
    CopysetNode* node_;
    braft::Closure* done_;
    OngoingConfChange confChange_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_
