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
#include <list>
#include <unordered_map>

#include "src/common/configuration.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/s3/metaserver_s3_adaptor.h"
#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::Configuration;
using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::LockGuard;
using ::curve::common::InterruptibleSleeper;
using ::curvefs::client::rpcclient::MdsClient;
using ::curvefs::client::rpcclient::MdsClientImpl;

struct TrashItem {
    uint32_t fsId;
    uint64_t inodeId;
    uint32_t dtime;
    bool deleted;
    TrashItem()
        : fsId(0),
          inodeId(0),
          dtime(0),
          deleted(false) {}
};

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

    virtual void Add(uint32_t fsId, uint64_t inodeId,
      uint32_t dtime, bool deleted = false) = 0;

    virtual void ListItems(std::list<TrashItem> *items) = 0;

    virtual void ScanTrash() = 0;

    virtual void StopScan() = 0;

    virtual bool IsStop() = 0;
};

class TrashImpl : public Trash {
 public:
    explicit TrashImpl(const std::shared_ptr<InodeStorage> &inodeStorage)
      : inodeStorage_(inodeStorage) {}

    ~TrashImpl() {}

    void Init(const TrashOption &option) override;

    void Add(uint32_t fsId, uint64_t inodeId,
      uint32_t dtime, bool deleted) override;

    void ListItems(std::list<TrashItem> *items) override;

    void ScanTrash() override;

    void StopScan() override;

    bool IsStop() override;

 private:
    bool NeedDelete(const TrashItem &item);

    MetaStatusCode DeleteInodeAndData(const TrashItem &item);

    uint64_t GetFsRecycleTimeHour(uint32_t fsId);

    void ClearDeleted(const TrashItem &item);

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<S3ClientAdaptor>  s3Adaptor_;
    std::shared_ptr<MdsClient> mdsClient_;
    std::unordered_map<uint32_t, FsInfo> fsInfoMap_;

    std::list<TrashItem> trashItems_;

    mutable Mutex itemsMutex_;

    TrashOption options_;

    mutable Mutex scanMutex_;

    bool isStop_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_TRASH_H_
