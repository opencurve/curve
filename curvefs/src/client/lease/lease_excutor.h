/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Created Date: Tue Mar 29 2022
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_
#define CURVEFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_

#include <memory>
#include <string>

#include "curvefs/src/client/rpcclient/metacache.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/common/config.h"
#include "src/client/lease_executor.h"

using curve::client::LeaseExecutorBase;
using curve::client::RefreshSessionTask;
using curvefs::client::common::LeaseOpt;
using curvefs::client::rpcclient::MdsClient;
using curvefs::client::rpcclient::MetaCache;
using curvefs::mds::Mountpoint;

namespace curvefs {
namespace client {

class LeaseExecutor : public LeaseExecutorBase {
 public:
    LeaseExecutor(const LeaseOpt &opt, std::shared_ptr<MetaCache> metaCache,
                  std::shared_ptr<MdsClient> mdsCli)
        : opt_(opt), metaCache_(metaCache), mdsCli_(mdsCli) {
         enableSumInDir_ = new bool();
        }

    ~LeaseExecutor() {
      delete(enableSumInDir_);
    }

    bool Start();

    void Stop();

    /**
     * refresh lease with mds and update resource
     */
    bool RefreshLease() override;

    void SetFsName(const std::string& fsName) {
       fsName_ = fsName;
    }

    void SetMountPoint(const Mountpoint& mp) {
       mountpoint_ = mp;
    }

    void SetEnableSumInDir(const bool& flag) {
       *enableSumInDir_ = flag;
    }

    bool EnableSumInDir() { return *enableSumInDir_; }

 private:
    LeaseOpt opt_;
    std::shared_ptr<MetaCache> metaCache_;
    std::shared_ptr<MdsClient> mdsCli_;
    std::unique_ptr<RefreshSessionTask> task_;
    std::string fsName_;
    Mountpoint mountpoint_;
    bool* enableSumInDir_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_
