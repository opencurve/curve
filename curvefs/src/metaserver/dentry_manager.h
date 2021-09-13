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

#include <list>
#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/transaction.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {
class DentryManager {
 public:
    DentryManager(std::shared_ptr<DentryStorage> dentryStorage,
                  std::shared_ptr<TxManager> txManger);

    MetaStatusCode CreateDentry(const Dentry& dentry);

    MetaStatusCode DeleteDentry(const Dentry& dentry);

    MetaStatusCode GetDentry(Dentry* dentry);

    MetaStatusCode ListDentry(const Dentry& dentry,
                              std::vector<Dentry>* dentrys,
                              uint32_t limit);

    MetaStatusCode HandleRenameTx(const std::vector<Dentry>& dentrys);

 private:
    void Log4Dentry(const std::string& request, const Dentry& dentry);
    void Log4Code(const std::string& request, MetaStatusCode rc);

 private:
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<TxManager> txManager_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_MANAGER_H_
