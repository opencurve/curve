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

#include <vector>
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/wait_interval.h"
#include "proto/scan.pb.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/copyset_node_manager.h"

using curve::common::Thread;
using curve::common::RWLock;
using curve::common::WaitInterval;

namespace curve {
namespace chunkserver {

typedef std::pair<LogicPoolID, CopysetID> ScanKey;

struct ScanManagerOptions {
    ChunkServerID chunkserverId;
    uint32_t intervalSec;
    uint64_t scanSize;
};

enum ScanType {
    Init,
    NewMap,
    BuildMap,
    WaitRep,
    CompareMap,
    Finish
};

struct ScanJob {
    ScanType type;
    CopysetID id;
    LogicPoolID poolId;
    uint64_t startTime;
    ScanMap localMap;
    std::vector<ScanMap> repMap;
    uint8_t watingNum;
    ChunkID currentChunkId;
    ScanJob() : type(ScanType::Init) {}
};

class ScanManager {
 public:
    ScanManager() {}
    virtual ~ScanManager();
    /**
     * @brief init scan Manager
     * @param[in] options scan Manager options
     * @return 0:successful, non-zero failed
     */
    int Init(const ScanManagerOptions options);
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

    void Enqueue(LogicPoolID poolId, CopysetID id);
    ScanKey Dequeue();
    int CancelScanJob(LogicPoolID poolId, CopysetID id);
    bool IsRepeatReq(LogicPoolID poolId, CopysetID id);
    void StartScanJob(ScanKey job);
    void SetScanJobType(ScanType type) {
        job_.type = type;        
    }
    void GenScanJob();
 private:
    ChunkID GetNextScanChunk();
    void ScanError();
    void ScanChunkReqProcess();
    void BuildLocalScanMap();
    bool isCurrentJobFinish();
    void ScanJobFinish();
    void CompareMap();
    // scan process thread
    Thread scanThread_;
    
    std::atomic<bool> toStop_;
    std::set<ScanKey> waitScanSet;
    RWLock rwLock_;
    WaitInterval waitInterval_;
    ScanJob job_;
    ChunkMap chunkMap_;
    std::shared_ptr<CSDataStore> dataStore_;
    CopysetNodeManager *copysetNodeManager_;
    int scanSize;
};
}
}

#endif