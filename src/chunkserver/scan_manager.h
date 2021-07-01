/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 21-4-8
 * Author: huyao
 */

#ifndef SRC_CHUNKSERVER_SCAN_MANAGER_H_
#define SRC_CHUNKSERVER_SCAN_MANAGER_H_

#include <google/protobuf/util/message_differencer.h>
#include <vector>
#include <memory>
#include <utility>
#include <set>
#include <map>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/wait_interval.h"
#include "proto/scan.pb.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/chunk_closure.h"

using curve::common::Thread;
using curve::common::RWLock;
using curve::common::WaitInterval;

namespace curve {
namespace chunkserver {

class ScanChunkRequest;

typedef std::pair<LogicPoolID, CopysetID> ScanKey;

/**
 * scan manager options
 */
struct ScanManagerOptions {
    uint32_t intervalSec;
    // once scan buf size
    uint64_t scanSize;
    uint32_t chunkMetaPageSize;
    // use for follower send scanmap to leader
    uint64_t timeoutMs;
    uint32_t retry;
    uint64_t retryIntervalUs;
    CopysetNodeManager* copysetNodeManager;
};

/**
 *  scan state machine type
 */
enum ScanType {
    Init,
    NewMap,
    WaitMap,
    CompareMap,
    Finish
};

struct ScanTask {
    ChunkID chunkId;
    uint64_t offset;
    uint8_t waitingNum;
    ScanMap localMap;
    std::vector<ScanMap> followerMap;
    ScanTask() : waitingNum(3) {}
};

struct ScanJob {
    ScanType type;
    CopysetID id;
    LogicPoolID poolId;
    ScanTask task;
    bool isFinished;
    RWLock taskLock;
    ChunkID currentChunkId;
    uint64_t currentOffset;
    ChunkMap chunkMap;
    std::shared_ptr<CSDataStore> dataStore;
    ScanJob() : type(ScanType::Init) {}
};

class ScanManager {
 public:
    ScanManager() {}
    virtual ~ScanManager() {}

    /**
     * @brief init scan Manager
     * @param[in] options scan Manager options
     * @return 0:successful, non-zero failed
     */
    int Init(const ScanManagerOptions& options);

    /**
     * @brief clean scan Manager
     * @return 0:successful, non-zero failed
     */
    int Fini();

    /**
     * @brief start scan Manager
     * @return 0:successful, non-zero failed
     */
    int Run();

    /**
     * @brief scan scanTask
     */
    void Scan();

     /**
     * @brief scan req in queue
     * @param[in] poolId: logicPool id
     * @param[in] id: copyset id 
     * @return 
     */
    void Enqueue(LogicPoolID poolId, CopysetID id);

    /**
     * @brief scan req out queue
     * @param[in] poolId: logicPool id
     * @param[in] id: copyset id 
     * @return ScanKey
     */
    ScanKey Dequeue();

    /**
     * @brief start scan job
     * @param[in] key: the scankey of job
     */
    void StartScanJob(ScanKey key);

    /**
     * @brief cancel scan job
     * @param[in] poolId: logicPool id
     * @param[in] id: copyset id
     * @return 0:successful, non-zero failed
     */
    int CancelScanJob(LogicPoolID poolId, CopysetID id);

    /**
     * @brief deal scan jobs within differert status
     * @param[in] key: the key of job
     */
    void GenScanJobs(ScanKey key);

    /**
     * @brief deal the scanmap received from follower
     * @param[in] request: follower send scanmap request
     * @param[in] response: response to the follower
     */
    void DealFollowerScanMap(const FollowScanMapRequest &request,
                             FollowScanMapResponse *response);

    /**
     * @brief set leader scanmap
     * @param[in] jobKey: the key of scanjob
     * @param[in] map: the leader scanmap
     */
    void SetLocalScanMap(ScanKey jobKey, ScanMap map);

    /**
     * @brief set scantype to job
     * @param[in] key: the key of job
     * @param[in] type: the scantype
     */
    void SetScanJobType(ScanKey key, ScanType type);

    // for test
    int GetWaitJobNum() {
        return waitScanSet_.size();
    }

    void SetJob(ScanKey key, std::shared_ptr<ScanJob> job) {
        jobMapLock_.WRLock();
        jobs_.emplace(key, job);
        jobMapLock_.Unlock();
    }

    int GetJobNum() {
        return jobs_.size();
    }

 private:
    /**
     * @brief process chunk scan request send task to braft
     * @param[in] job: the scan job
     * @return 0 is finished, -1 is canceled
     */
    int ScanJobProcess(const std::shared_ptr<ScanJob> job);

    /**
     * @brief set the scan job to finished status
     * @param[in] job: the scan job
     */
    void ScanJobFinish(std::shared_ptr<ScanJob> job);

    /**
     * @brief deal a scan job
     * @param[in] job: the scan job need to deal with
     * @return true means go out from statu mechain, false is continue
     */
    bool GenScanJob(std::shared_ptr<ScanJob> job);

    /**
     * @brief compare the scanmap from three copies
     * @param[in] job: the scan job
     */
    void CompareMap(std::shared_ptr<ScanJob> job);

    /**
     * @brief get scan job based key
     * @param[in] key: the key of scan job
     * @return the scan job
     */
    std::shared_ptr<ScanJob> GetJob(ScanKey key);

    // scan process thread
    Thread scanThread_;
    std::atomic<bool> toStop_;
    std::set<ScanKey> waitScanSet_;
    RWLock waitSetLock_;
    WaitInterval jobWaitInterval_;
    WaitInterval scanTaskWaitInterval_;
    std::map<ScanKey, std::shared_ptr<ScanJob>> jobs_;
    RWLock jobMapLock_;
    CopysetNodeManager *copysetNodeManager_;
    uint32_t chunkSize_;
    uint32_t chunkMetaPageSize_;
    uint64_t scanSize_;
    uint64_t timeoutMs_;
    uint32_t retry_;
    uint64_t retryIntervalUs_;
};
}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_SCAN_MANAGER_H_
