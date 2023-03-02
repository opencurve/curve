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
 * Created Date: 2022-12-21
 * Author: hzchenwei7
 */

#include "curvefs/src/metaserver/fsinfo_manager.h"

namespace curvefs {
namespace metaserver {
bool FsInfoManager::GetFsInfo(uint32_t fsId, FsInfo *fsInfo) {
    if (fsInfoMap_.find(fsId) == fsInfoMap_.end()) {
        auto ret = mdsClient_->GetFsInfo(fsId, fsInfo);
        if (ret != FSStatusCode::OK) {
            if (FSStatusCode::NOT_FOUND == ret) {
                LOG(ERROR) << "The fsName not exist, fsId = " << fsId;
                return false;
            } else {
                LOG(ERROR)
                    << "GetFsInfo failed, FSStatusCode = " << ret
                    << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                    << ", fsId = " << fsId;
                return false;
            }
        }
        fsInfoMap_.insert({fsId, *fsInfo});
    } else {
        *fsInfo = fsInfoMap_.find(fsId)->second;
    }

    return true;
}
}  // namespace metaserver
}  // namespace curvefs