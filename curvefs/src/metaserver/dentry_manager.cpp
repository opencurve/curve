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
    LOG(INFO) << "CreateDentry, fsId = " << dentry.fsid()
              << ", inodeId = " << dentry.inodeid()
              << ", parentId = " << dentry.parentinodeid()
              << ", name = " << dentry.name();
    MetaStatusCode status = dentryStorage_->Insert(dentry);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateDentry fail, fsId = " << dentry.fsid()
              << ", inodeId = " << dentry.inodeid()
              << ", parentId = " << dentry.parentinodeid()
              << ", name = " << dentry.name()
              << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    LOG(INFO) << "CreateDentry success, fsId = " << dentry.fsid()
              << ", inodeId = " << dentry.inodeid()
              << ", parentId = " << dentry.parentinodeid()
              << ", name = " << dentry.name();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::GetDentry(uint32_t fsId,
                                        uint64_t parentId,
                                        std::string name, Dentry *dentry) {
    LOG(INFO) << "GetDentry, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name;
    MetaStatusCode status = dentryStorage_->Get(DentryKey(fsId, parentId, name),
                                    dentry);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "GetDentry fail, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name
              << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    LOG(INFO) << "GetDentry success, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::DeleteDentry(uint32_t fsId, uint64_t parentId,
                                        std::string name) {
    LOG(INFO) << "DeleteDentry, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name;
    MetaStatusCode status = dentryStorage_->Delete(
                                    DentryKey(fsId, parentId, name));
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "DeleteDentry fail, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name
              << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    LOG(INFO) << "DeleteDentry success, fsId = " << fsId
              << ", parentId = " << parentId
              << ", name = " << name;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryManager::ListDentry(uint32_t fsId, uint64_t dirId,
                                        std::list<Dentry>* dentryList) {
    LOG(INFO) << "ListDentry, fsId = " << fsId
              << ", dirId = " << dirId;
    MetaStatusCode status = dentryStorage_->List(
                                DentryParentKey(fsId, dirId), dentryList);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "ListDentry fail, fsId = " << fsId
              << ", dirId = " << dirId
              << ", ret = " << MetaStatusCode_Name(status);
        return status;
    }

    LOG(INFO) << "ListDentry success, fsId = " << fsId
              << ", dirId = " << dirId
              << ", get count = " << dentryList->size();
    for (auto it : *dentryList) {
        LOG(INFO) << it.DebugString();
    }
    return MetaStatusCode::OK;
}
}  // namespace metaserver
}  // namespace curvefs
