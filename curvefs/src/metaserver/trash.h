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
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_METASERVER_TRASH_H_
#define CURVEFS_SRC_METASERVER_TRASH_H_

#include <cstdint>
#include <memory>
#include <map>
#include <list>
#include <unordered_map>

#include "src/common/configuration.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/s3/metaserver_s3_adaptor.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/metaserver/common/types.h"

namespace curvefs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

using ::curve::common::Configuration;
using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::LockGuard;
using ::curve::common::InterruptibleSleeper;
using ::curvefs::client::rpcclient::MdsClient;
using ::curvefs::client::rpcclient::MdsClientImpl;

struct TrashOption {
    uint32_t scanPeriodSec;
    uint32_t expiredAfterSec;
    std::shared_ptr<S3ClientAdaptor>  s3Adaptor;
    std::shared_ptr<MdsClient> mdsClient;
    TrashOption()
      : scanPeriodSec(0),
        expiredAfterSec(0),
        s3Adaptor(nullptr),
        mdsClient(nullptr) {}

    void InitTrashOptionFromConf(std::shared_ptr<Configuration> conf);
};

class Trash {
 public:
    Trash() {}
    virtual ~Trash() {}

    virtual void Init(const TrashOption &option) = 0;

    virtual void Add(uint64_t inodeId,
      uint64_t dtime, bool deleted = false) = 0;

    virtual void Remove(uint64_t inodeId) = 0;

    virtual uint64_t Size() = 0;

    virtual void ScanTrash() = 0;

    virtual void StopScan() = 0;

    virtual bool IsStop() = 0;
};

class TrashImpl : public Trash {
 public:
    explicit TrashImpl(const std::shared_ptr<InodeStorage> &inodeStorage,
        uint32_t fsId = 0, PoolId poolId = 0, CopysetId copysetId = 0,
        PartitionId partitionId = 0) :
        inodeStorage_(inodeStorage), fsId_(fsId), poolId_(poolId),
        copysetId_(copysetId), partitionId_(partitionId) {}

    ~TrashImpl() {}

    void Init(const TrashOption &option) override;

    void Add(uint64_t inodeId, uint64_t dtime, bool deleted = false) override;

    void Remove(uint64_t inodeId) override;

    uint64_t Size() override;

    void ScanTrash() override;

    void StopScan() override;

    bool IsStop() override;

    // for utests
    void SetCopysetNode(const std::shared_ptr<copyset::CopysetNode> &node) {
        copysetNode_ = node;
    }

 private:
    bool NeedDelete(uint64_t dtime);

    uint64_t GetFsRecycleTimeHour(uint32_t fsId);

    MetaStatusCode DeleteInodeAndData(uint64_t inodeId);

    MetaStatusCode DeleteInode(uint64_t inodeId);

    void RemoveDeletedInode(uint64_t inodeId);

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<S3ClientAdaptor>  s3Adaptor_;
    std::shared_ptr<MdsClient> mdsClient_;
    std::shared_ptr<copyset::CopysetNode> copysetNode_;
    FsInfo fsInfo_;

    uint32_t fsId_;
    PoolId poolId_;
    CopysetId copysetId_;
    PartitionId partitionId_;

    std::unordered_map<uint64_t, uint64_t> trashItems_;

    mutable Mutex itemsMutex_;

    TrashOption options_;

    mutable Mutex scanMutex_;

    bool isStop_;
};

class DeleteInodeClosure : public google::protobuf::Closure {
 private:
    std::mutex mutex_;
    std::condition_variable cond_;
    bool runned_ = false;

 public:
    void Run() override {
        std::lock_guard<std::mutex> l(mutex_);
        runned_ = true;
        cond_.notify_one();
    }

    void WaitRunned() {
        std::unique_lock<std::mutex> ul(mutex_);
        cond_.wait(ul, [this]() { return runned_; });
    }
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_TRASH_H_
