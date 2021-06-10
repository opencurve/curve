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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */
#include "curvefs/src/client/dentry_cache_manager.h"

#include <string>
#include <list>
#include <unordered_map>

namespace curvefs {
namespace client {

CURVEFS_ERROR DentryCacheManager::GetDentry(
    uint64_t inodeid, const std::string &name, Dentry *out) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManager::CreateDentry(const Dentry &dentry) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManager::DeleteDentry(
    uint64_t inodeid, const std::string &name) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManager::ListDentry(
    uint64_t parent, std::list<Dentry> *dentryList) {
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
