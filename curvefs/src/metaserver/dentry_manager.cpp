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

#include "curvefs/src/metaserver/dentry_manager.h"
#include <glog/logging.h>

namespace curvefs {
namespace metaserver {
MetaStatusCode DentryManager::CreateDentry(const Dentry& dentry) {
    VLOG(1) << "CreateDentry, dentry: " << dentry.ShortDebugString();
    MetaStatusCode status = dentryStorage_->Insert(dentry);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateDentry fail, dentry: " << dentry.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    VLOG(1) << "CreateDentry success, dentry: " << dentry.ShortDebugString();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::GetDentry(uint32_t fsId, uint64_t parentId,
                                        const std::string& name,
                                        Dentry* dentry) {
    VLOG(1) << "GetDentry, fsId = " << fsId << ", parentId = " << parentId
              << ", name = " << name;
    MetaStatusCode status =
        dentryStorage_->Get(DentryKey(fsId, parentId, name), dentry);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "GetDentry fail, fsId = " << fsId
                   << ", parentId = " << parentId << ", name = " << name
                   << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    VLOG(1) << "GetDentry success, fsId = " << fsId
              << ", parentId = " << parentId << ", name = " << name;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::DeleteDentry(uint32_t fsId, uint64_t parentId,
                                           const std::string& name) {
    VLOG(1) << "DeleteDentry, fsId = " << fsId << ", parentId = " << parentId
              << ", name = " << name;
    MetaStatusCode status =
        dentryStorage_->Delete(DentryKey(fsId, parentId, name));
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "DeleteDentry fail, fsId = " << fsId
                   << ", parentId = " << parentId << ", name = " << name
                   << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    VLOG(1) << "DeleteDentry success, fsId = " << fsId
              << ", parentId = " << parentId << ", name = " << name;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::ListDentry(uint32_t fsId, uint64_t dirId,
                                         std::list<Dentry>* dentryList) {
    VLOG(1) << "ListDentry, fsId = " << fsId << ", dirId = " << dirId;
    MetaStatusCode status =
        dentryStorage_->List(DentryParentKey(fsId, dirId), dentryList);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "ListDentry fail, fsId = " << fsId
                   << ", dirId = " << dirId
                   << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    VLOG(1) << "ListDentry success, fsId = " << fsId << ", dirId = " << dirId
              << ", get count = " << dentryList->size();
    for (auto it : *dentryList) {
        VLOG(1) << it.ShortDebugString();
    }
    return MetaStatusCode::OK;
}
}  // namespace metaserver
}  // namespace curvefs
