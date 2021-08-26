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

#include <sstream>

#include "curvefs/src/metaserver/dentry_manager.h"

namespace curvefs {
namespace metaserver {

DentryManager::DentryManager(std::shared_ptr<DentryStorage> dentryStorage)
    : dentryStorage_(dentryStorage) {
    txManager_ = std::make_shared<TxManager>(dentryStorage);
}

void DentryManager::Log(const std::string& request, const Dentry& dentry) {
    VLOG(1) << "Receive " <<  request << " request, dentry = ("
            << dentry.ShortDebugString() << ")";
}

void DentryManager::Log(const std::string& request, MetaStatusCode rc) {
    auto succ = (rc == MetaStatusCode::OK);
    std::ostringstream message;
    message << request << " " << (succ ? "success" : "fail")
            << ", retCode = " << MetaStatusCode_Name(rc);

    if (succ) {
        VLOG(1) << message.str();
    } else {
        LOG(ERROR) << message.str();
    }
}

MetaStatusCode DentryManager::CreateDentry(const Dentry& dentry) {
    Log("CreateDentry", dentry);
    auto rc = dentryStorage_->Insert(dentry);
    Log("CreateDentry", rc);
    return rc;
}

MetaStatusCode DentryManager::DeleteDentry(const Dentry& dentry) {
    Log("DeleteDentry", dentry);
    auto rc = dentryStorage_->Delete(dentry);
    Log("DeleteDentry", rc);
    return rc;
}

MetaStatusCode DentryManager::GetDentry(Dentry* dentry) {
    Log("GetDentry", *dentry);
    auto rc = dentryStorage_->Get(dentry);
    Log("GetDentry", rc);
    return rc;
}

MetaStatusCode DentryManager::ListDentry(const Dentry& dentry,
                                         std::vector<Dentry>* dentrys,
                                         uint32_t limit) {
    Log("ListDentry", dentry);
    auto rc = dentryStorage_->List(dentry, dentrys, limit);
    Log("ListDentry", rc);
    return rc;
}

MetaStatusCode DentryManager::HandleRenameTx(
    const std::vector<Dentry>& dentrys) {
    for (const auto& dentry : dentrys) {
        Log("HandleRenameTx", dentry);
    }
    auto rc = txManager_->HandleRenameTx(dentrys);
    Log("HandleRenameTx", rc);
    return rc;
}

}  // namespace metaserver
}  // namespace curvefs
