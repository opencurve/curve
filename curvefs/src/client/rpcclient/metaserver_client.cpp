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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#include <algorithm>
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {

MetaStatusCode MetaServerClientImpl::Init(const MetaServerOption &metaopt,
                                          MetaServerBaseClient *baseclient) {
    return MetaStatusCode::OK;
}


MetaStatusCode MetaServerClientImpl::GetDentry(uint32_t fsId, uint64_t inodeid,
                                               const std::string &name,
                                               Dentry *out) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
                                                const std::string &last,
                                                uint32_t count,
                                                std::list<Dentry> *dentryList) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::DeleteDentry(uint32_t fsId,
                                                  uint64_t inodeid,
                                                  const std::string &name) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::GetInode(uint32_t fsId, uint64_t inodeid,
                                              Inode *out) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::UpdateInode(const Inode &inode) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::CreateInode(const InodeParam &param,
                                                 Inode *out) {
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::DeleteInode(uint32_t fsId,
                                                 uint64_t inodeid) {
    return MetaStatusCode::OK;
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
