/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Wed Mar 22 10:40:08 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_MDS_FSINFO_MANAGER_H_
#define CURVEFS_SRC_METASERVER_MDS_FSINFO_MANAGER_H_

#include <map>
#include <memory>
#include <mutex>
#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::client::rpcclient::MdsClient;

class FsInfoManager {
 public:
    static FsInfoManager &GetInstance() {
        static FsInfoManager instance_;
        return instance_;
    }

    void SetMdsClient(std::shared_ptr<MdsClient> mdsClient) {
        mdsClient_ = mdsClient;
    }

    bool GetFsInfo(uint32_t fsId, FsInfo *fsInfo);

 private:
    std::shared_ptr<MdsClient> mdsClient_;
    std::map<uint32_t, FsInfo> fsInfoMap_;

    std::mutex mtx_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_MDS_FSINFO_MANAGER_H_
