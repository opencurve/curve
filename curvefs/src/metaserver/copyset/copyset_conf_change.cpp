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
 * Date: Tuesday Nov 23 11:20:12 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_conf_change.h"

#include "curvefs/src/metaserver/copyset/copyset_node.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

void OnConfChangeDone::Run() {
    node_->OnConfChangeComplete();

    LOG_IF(WARNING, !status().ok())
        << "Copyset: " << node_->Name() << " "
        << curve::mds::heartbeat::ConfigChangeType_Name(confChange_.type)
        << " failed";

    if (done_) {
        done_->status() = status();
        done_->Run();
    }

    delete this;
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
