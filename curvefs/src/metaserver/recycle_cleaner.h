/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Project: curve
 * @Date: 2022-08-25 15:39:29
 * @Author: chenwei
 */
#ifndef CURVEFS_SRC_METASERVER_RECYCLE_CLEANER_H_
#define CURVEFS_SRC_METASERVER_RECYCLE_CLEANER_H_

#include <memory>
#include <string>

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/partition.h"

namespace curvefs {
namespace metaserver {
using curvefs::client::rpcclient::MetaServerClient;
class RecycleCleaner {
 public:
    explicit RecycleCleaner(const std::shared_ptr<Partition> &partition)
        : partition_(partition) {
        isStop_ = false;
        LOG(INFO) << "RecycleCleaner poolId = " << partition->GetPoolId()
                  << ", fsId = " << partition->GetFsId()
                  << ", partitionId = " << partition->GetPartitionId();
    }

    void SetCopysetNode(copyset::CopysetNode *copysetNode) {
        copysetNode_ = copysetNode;
    }

    void SetMdsClient(std::shared_ptr<MdsClient> mdsClient) {
        mdsClient_ = mdsClient;
    }

    void SetMetaClient(std::shared_ptr<MetaServerClient> metaClient) {
        metaClient_ = metaClient;
    }

    void SetScanLimit(uint32_t limit) {
        limit_ = limit;
    }

    // scan recycle dir and delete expired files
    bool ScanRecycle();
    // if dir is timeout, return true
    bool IsDirTimeOut(const std::string &dir);
    uint32_t GetRecycleTime() {
        return fsInfo_.has_recycletimehour() ? fsInfo_.recycletimehour() : 0;
    }
    bool GetEnableSumInDir() { return fsInfo_.enablesumindir(); }
    // delete dir and all files in dir recursively
    bool DeleteDirRecursive(const Dentry &dentry);
    // update fs info every time it's called to get lastest recycle time
    bool UpdateFsInfo();
    // delete one file or one dir directly
    bool DeleteNode(const Dentry &dentry);

    uint32_t GetPartitionId() {
        return partition_->GetPartitionId();
    }

    uint32_t GetFsId() { return partition_->GetFsId(); }

    void Stop() { isStop_ = true; }

    bool IsStop() { return isStop_; }

    uint64_t GetTxId();

 private:
    std::shared_ptr<Partition> partition_;
    copyset::CopysetNode *copysetNode_;
    bool isStop_;
    std::shared_ptr<MdsClient> mdsClient_;
    std::shared_ptr<MetaServerClient> metaClient_;
    FsInfo fsInfo_;
    uint64_t limit_ = 1000;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_RECYCLE_CLEANER_H_
