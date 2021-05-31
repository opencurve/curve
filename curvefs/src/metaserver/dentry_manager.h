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

#ifndef CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_
#define CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_

#include <string>
#include <memory>
#include <atomic>
#include <list>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {
class DentryManager {
 public:
    explicit DentryManager(std::shared_ptr<DentryStorage> dentryStorage) {
       dentryStorage_ = dentryStorage;
    }

    MetaStatusCode CreateDentry(const Dentry& dentry);

    MetaStatusCode GetDentry(uint32_t fsId, uint64_t parentId,
                                        std::string name, Dentry *dentry);

    MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t parentId,
                                        std::string name);

    MetaStatusCode ListDentry(uint32_t fsId, uint64_t dirId,
                                        std::list<Dentry>* dentryList);

 private:
    std::shared_ptr<DentryStorage> dentryStorage_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_
