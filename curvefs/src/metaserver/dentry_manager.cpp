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
 * Created Date: 2021-05-19
 * Author: chenwei
 */
#include <glog/logging.h>
#include "curvefs/src/metaserver/dentry_manager.h"

namespace curvefs {
namespace metaserver {
MetaStatusCode DentryManager::CreateDentry(const Dentry& dentry) {
    MetaStatusCode status = dentryStorage_->Insert(dentry);
    return status;
}

MetaStatusCode DentryManager::GetDentry(uint32_t fsId,
                                        uint64_t parentId,
                                        std::string name, Dentry *dentry) {
    MetaStatusCode status = dentryStorage_->Get(DentryKey(fsId, parentId, name),
                                    dentry);
    return status;
}

MetaStatusCode DentryManager::DeleteDentry(uint32_t fsId, uint64_t parentId,
                                        std::string name) {
    MetaStatusCode status = dentryStorage_->Delete(
                                    DentryKey(fsId, parentId, name));
    return status;
}

MetaStatusCode DentryManager::ListDentry(uint32_t fsId, uint64_t dirId,
                                        std::list<Dentry>* dentryList) {
    MetaStatusCode status = dentryStorage_->List(
                                DentryParentKey(fsId, dirId), dentryList);
    return status;
}
}  // namespace metaserver
}  // namespace curvefs
