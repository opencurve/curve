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
 * Created Date: Monday June 10th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_

#include <bvar/bvar.h>
#include <butil/time.h>
#include <string>
#include <unordered_map>
#include <memory>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/uncopyable.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/configuration.h"
#include "src/chunkserver/datastore/file_pool.h"

using curve::common::Configuration;
using curve::common::ReadLockGuard;
using curve::common::RWLock;
using curve::common::Uncopyable;
using curve::common::WriteLockGuard;

namespace curve {
namespace chunkserver {

class CopysetNodeManager;
class FilePool;
class CSDataStore;
class CurveSegmentLogStorage;
class Trash;

template <typename Tp>
using PassiveStatusPtr = std::shared_ptr<bvar::PassiveStatus<Tp>>;

template <typename Tp> using AdderPtr = std::shared_ptr<bvar::Adder<Tp>>;

//Using the implementation of LatencyRecorder to count the size of read and write requests
//Statistics can be conducted on quantile values, maximum values, median values, mean values, and other factors
using IOSizeRecorder = bvar::LatencyRecorder;

//IO related statistical items
class IOMetric {
 public:
    IOMetric();
    virtual ~IOMetric();
    /**
     *Initialize io metric
     *Mainly used for exposing various metric indicators
     * @param prefix: The prefix used for bvar exposure
     * @return returns 0 for success, -1 for failure
     */
    int Init(const std::string &prefix);
    /**
     *Count requestNum when IO requests arrive
     */
    void OnRequest();
    /**
     *After IO is completed, record the indicators for this IO
     *Incorrect IO will not be included in iops and bps statistics
     * @param size: The size of the IO data for this time
     * @param latUS: The delay of this IO
     * @param hasError: Did any errors occur during this IO
     */
    void OnResponse(size_t size, int64_t latUs, bool hasError);

 public:
    //Number of IO requests
    bvar::Adder<uint64_t> reqNum_;
    //Number of successful IO
    bvar::Adder<uint64_t> ioNum_;
    //Number of failed IO
    bvar::Adder<uint64_t> errorNum_;
    //The data volume of all IO
    bvar::Adder<uint64_t> ioBytes_;
    //Delay situation of IO (quantile, maximum, median, average)
    bvar::LatencyRecorder latencyRecorder_;
    //The size of IO (quantile, maximum, median, average)
    IOSizeRecorder sizeRecorder_;
    //Number of IO requests in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>> rps_;
    //iops in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>> iops_;
    //Number of IO errors in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>> eps_;
    //Data volume in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>> bps_;
};
using IOMetricPtr = std::shared_ptr<IOMetric>;

enum class CSIOMetricType {
    READ_CHUNK = 0,
    WRITE_CHUNK = 1,
    RECOVER_CHUNK = 2,
    PASTE_CHUNK = 3,
    DOWNLOAD = 4,
};

class CSIOMetric {
 public:
    CSIOMetric()
        : readMetric_(nullptr), writeMetric_(nullptr), recoverMetric_(nullptr),
          pasteMetric_(nullptr), downloadMetric_(nullptr) {}

    ~CSIOMetric() {}

    /**
     *Record metric before executing the request
     * @param type: The corresponding metric type of the request
     */
    void OnRequest(CSIOMetricType type);

    /**
     *Record metric after executing the request
     *Incorrect IO will not be included in iops and bps statistics
     * @param type: The corresponding metric type of the request
     * @param size: The size of the IO data for this time
     * @param latUS: The delay of this IO
     * @param hasError: Did any errors occur during this IO
     */
    void OnResponse(CSIOMetricType type, size_t size, int64_t latUs,
                    bool hasError);

    /**
     *Obtain IOMetric of the specified type
     * @param type: The corresponding metric type of the request
     * @return returns the IOMetric pointer corresponding to the specified type, or nullptr if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type);

    /**
     *Initialize metric statistics for each op
     * @return returns 0 for success, -1 for failure
     */
    int Init(const std::string &prefix);
    /**
     *Release metric resources for various OPs
     */
    void Fini();

 protected:
    //ReadChunk statistics
    IOMetricPtr readMetric_;
    //WriteChunk statistics
    IOMetricPtr writeMetric_;
    //RecoverChunk statistics
    IOMetricPtr recoverMetric_;
    //PasteChunk Information
    IOMetricPtr pasteMetric_;
    //Download statistics
    IOMetricPtr downloadMetric_;
};

class CSCopysetMetric {
 public:
    CSCopysetMetric()
        : logicPoolId_(0), copysetId_(0), chunkCount_(nullptr),
          walSegmentCount_(nullptr), snapshotCount_(nullptr),
          cloneChunkCount_(nullptr) {}

    ~CSCopysetMetric() {}

    /**
     *Initialize metric statistics at the copyset level
     * @param logicPoolId: The ID of the logical pool to which the copyset belongs
     * @param copysetId: The ID of the copyset
     * @return returns 0 for success, -1 for failure
     */
    int Init(const LogicPoolID &logicPoolId, const CopysetID &copysetId);

    /**
     *Monitor Datastore indicators, mainly including the number of chunks, number of snapshots, etc
     * @param datastore: The datastore pointer under this copyset
     */
    void MonitorDataStore(CSDataStore *datastore);

    /**
     * @brief: Monitor log storage's metric, like the number of WAL segment file
     * @param logStorage: The pointer to CurveSegmentLogStorage
     */
    void MonitorCurveSegmentLogStorage(CurveSegmentLogStorage *logStorage);

    /**
     *Record metric before executing the request
     * @param type: The corresponding metric type of the request
     */
    void OnRequest(CSIOMetricType type) { ioMetrics_.OnRequest(type); }

    /**
     *Record metric after executing the request
     *Incorrect IO will not be included in iops and bps statistics
     * @param type: The corresponding metric type of the request
     * @param size: The size of the IO data for this time
     * @param latUS: The delay of this IO
     * @param hasError: Did any errors occur during this IO
     */
    void OnResponse(CSIOMetricType type, size_t size, int64_t latUs,
                    bool hasError) {
        ioMetrics_.OnResponse(type, size, latUs, hasError);
    }

    /**
     *Obtain IOMetric of the specified type
     * @param type: The corresponding metric type of the request
     * @return returns the IOMetric pointer corresponding to the specified type, or nullptr if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type) {
        return ioMetrics_.GetIOMetric(type);
    }

    uint32_t GetChunkCount() const {
        if (chunkCount_ == nullptr) {
            return 0;
        }
        return chunkCount_->get_value();
    }

    uint32_t GetWalSegmentCount() const {
        if (nullptr == walSegmentCount_) {
            return 0;
        }
        return walSegmentCount_->get_value();
    }

    uint32_t GetSnapshotCount() const {
        if (snapshotCount_ == nullptr) {
            return 0;
        }
        return snapshotCount_->get_value();
    }

    uint32_t GetCloneChunkCount() const {
        if (cloneChunkCount_ == nullptr) {
            return 0;
        }
        return cloneChunkCount_->get_value();
    }

 private:
    inline std::string Prefix() {
        return "copyset_" + std::to_string(logicPoolId_) + "_" +
               std::to_string(copysetId_);
    }

 private:
    //Logical Pool ID
    LogicPoolID logicPoolId_;
    // copyset id
    CopysetID copysetId_;
    //Number of chunks on copyset
    PassiveStatusPtr<uint32_t> chunkCount_;
    // The total number of WAL segment in copyset
    PassiveStatusPtr<uint32_t> walSegmentCount_;
    //Number of snapshot files on copyset
    PassiveStatusPtr<uint32_t> snapshotCount_;
    //The number of clone chunks on the copyset
    PassiveStatusPtr<uint32_t> cloneChunkCount_;
    //Metric statistics of IO types on copyset
    CSIOMetric ioMetrics_;
};

struct ChunkServerMetricOptions {
    bool collectMetric;
    //Chunkserver IP
    std::string ip;
    //The port number of chunkserver
    uint32_t port;
    ChunkServerMetricOptions()
        : collectMetric(false), ip("127.0.0.1"), port(8888) {}
};

using CopysetMetricPtr = std::shared_ptr<CSCopysetMetric>;

class CopysetMetricMap {
 public:
    CopysetMetricMap() = default;
    ~CopysetMetricMap() = default;

    void Add(GroupId groupId, CopysetMetricPtr metric) {
        WriteLockGuard lockGuard(rwLock_);
        auto it = map_.find(groupId);
        if (it == map_.end()) {
            map_[groupId] = metric;
        }
    }

    void Remove(GroupId groupId) {
        WriteLockGuard lockGuard(rwLock_);
        auto it = map_.find(groupId);
        if (it != map_.end()) {
            map_.erase(it);
        }
    }

    CopysetMetricPtr Get(GroupId groupId) {
        ReadLockGuard lockGuard(rwLock_);
        auto it = map_.find(groupId);
        if (it == map_.end()) {
            return nullptr;
        }
        return it->second;
    }

    bool Exist(GroupId groupId) {
        ReadLockGuard lockGuard(rwLock_);
        auto it = map_.find(groupId);
        return it != map_.end();
    }

    uint32_t Size() {
        ReadLockGuard lockGuard(rwLock_);
        return map_.size();
    }

    void Clear() {
        WriteLockGuard lockGuard(rwLock_);
        map_.clear();
    }

    std::unordered_map<GroupId, CopysetMetricPtr> GetMap() {
        ReadLockGuard lockGuard(rwLock_);
        return map_;
    }

 private:
    //Protect the read write lock of the replication group metric map
    RWLock rwLock_;
    //Mapping table for each replication group metric, using GroupId as the key
    std::unordered_map<GroupId, CopysetMetricPtr> map_;
};

class ChunkServerMetric : public Uncopyable {
 public:
    //Implementation singleton
    static ChunkServerMetric *GetInstance();

    /**
     *Initialize chunkserver statistics
     * @param option: Initialize configuration item
     * @return returns 0 for success, -1 for failure
     */
    int Init(const ChunkServerMetricOptions &option);

    /**
     *Release metric resources
     * @return returns 0 for success, -1 for failure
     */
    int Fini();

    /**
     *Record metric before request
     * @param logicPoolId: The logical pool ID where this io operation is located
     * @param copysetId: The copysetID where this io operation is located
     * @param type: Request type
     */
    void OnRequest(const LogicPoolID &logicPoolId, const CopysetID &copysetId,
                   CSIOMetricType type);

    /**
     *Record the IO metric at the end of the request
     *Incorrect IO will not be included in iops and bps statistics
     * @param logicPoolId: The logical pool ID where this io operation is located
     * @param copysetId: The copysetID where this io operation is located
     * @param type: Request type
     * @param size: The size of the IO data for this time
     * @param latUS: The delay of this IO
     * @param hasError: Did any errors occur during this IO
     */
    void OnResponse(const LogicPoolID &logicPoolId, const CopysetID &copysetId,
                    CSIOMetricType type, size_t size, int64_t latUs,
                    bool hasError);

    /**
     *Create a metric for the specified copyset
     *If collectMetric is false, it returns 0, but it is not actually created
     * @param logicPoolId: The ID of the logical pool to which the copyset belongs
     * @param copysetId: The ID of the copyset
     * @return returns 0 for success, -1 for failure, or failure if the specified metric already exists
     */
    int CreateCopysetMetric(const LogicPoolID &logicPoolId,
                            const CopysetID &copysetId);

    /**
     *Obtain the metric of the specified copyset
     * @param logicPoolId: The ID of the logical pool to which the copyset belongs
     * @param copysetId: The ID of the copyset
     * @return successfully returns the specified copyset metric, while failure returns nullptr
     */
    CopysetMetricPtr GetCopysetMetric(const LogicPoolID &logicPoolId,
                                      const CopysetID &copysetId);

    /**
     *Delete the metric for the specified copyset
     * @param logicPoolId: The ID of the logical pool to which the copyset belongs
     * @param copysetId: The ID of the copyset
     * @return returns 0 for success, -1 for failure
     */
    int RemoveCopysetMetric(const LogicPoolID &logicPoolId,
                            const CopysetID &copysetId);

    /**
     *Monitor the chunk allocation pool, mainly monitoring the number of chunks in the pool
     * @param chunkFilePool: Object pointer to chunkfilePool
     */
    void MonitorChunkFilePool(FilePool *chunkFilePool);

    /**
     *Monitor the allocation pool of wall segments, mainly monitoring the number of segments in the pool
     * @param walFilePool: Object pointer to walFilePool
     */
    void MonitorWalFilePool(FilePool *walFilePool);

    /**
     *Monitor Recycle Bin
     * @param trash: Object pointer to trash
     */
    void MonitorTrash(Trash *trash);

    /**
     *Increase the leader count count
     */
    void IncreaseLeaderCount();

    /**
     *Reduce leader count count
     */
    void DecreaseLeaderCount();

    /**
     *Update configuration item data
     * @param conf: Configuration content
     */
    void ExposeConfigMetric(common::Configuration *conf);

    /**
     *Obtain IOMetric of the specified type
     * @param type: The corresponding metric type of the request
     * @return returns the IOMetric pointer corresponding to the specified type, or nullptr if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type) {
        return ioMetrics_.GetIOMetric(type);
    }

    CopysetMetricMap *GetCopysetMetricMap() { return &copysetMetricMap_; }

    uint32_t GetCopysetCount() { return copysetMetricMap_.Size(); }

    uint32_t GetLeaderCount() const {
        if (leaderCount_ == nullptr)
            return 0;
        return leaderCount_->get_value();
    }

    uint32_t GetTotalChunkCount() {
        if (chunkCount_ == nullptr)
            return 0;
        return chunkCount_->get_value();
    }

    uint32_t GetTotalSnapshotCount() {
        if (snapshotCount_ == nullptr)
            return 0;
        return snapshotCount_->get_value();
    }

    uint32_t GetTotalCloneChunkCount() {
        if (cloneChunkCount_ == nullptr)
            return 0;
        return cloneChunkCount_->get_value();
    }

    uint32_t GetTotalWalSegmentCount() {
        if (nullptr == walSegmentCount_)
            return 0;
        return walSegmentCount_->get_value();
    }

    uint32_t GetChunkLeftCount() const {
        if (chunkLeft_ == nullptr)
            return 0;
        return chunkLeft_->get_value();
    }

    uint32_t GetWalSegmentLeftCount() const {
        if (nullptr == walSegmentLeft_)
            return 0;
        return walSegmentLeft_->get_value();
    }

    uint32_t GetChunkTrashedCount() const {
        if (chunkTrashed_ == nullptr)
            return 0;
        return chunkTrashed_->get_value();
    }

 private:
    ChunkServerMetric();

    inline std::string Prefix() {
        return "chunkserver_" + option_.ip + "_" + std::to_string(option_.port);
    }

 private:
    //Initialization flag

    bool hasInited_;
    //Configuration Item

    ChunkServerMetricOptions option_;
    //Number of leaders

    AdderPtr<uint32_t> leaderCount_;
    //The number of remaining chunks in the chunkfilepool

    PassiveStatusPtr<uint32_t> chunkLeft_;
    //The number of remaining wal segments in the walfilepool

    PassiveStatusPtr<uint32_t> walSegmentLeft_;
    //Number of chunks in trash

    PassiveStatusPtr<uint32_t> chunkTrashed_;
    //Number of chunks on chunkserver

    PassiveStatusPtr<uint32_t> chunkCount_;
    // The total number of WAL segment in chunkserver
    PassiveStatusPtr<uint32_t> walSegmentCount_;
    //Number of snapshot files on chunkserver

    PassiveStatusPtr<uint32_t> snapshotCount_;
    //Number of clone chunks on chunkserver

    PassiveStatusPtr<uint32_t> cloneChunkCount_;
    //Mapping table for each replication group metric, using GroupId as the key

    CopysetMetricMap copysetMetricMap_;
    //Metric statistics of IO types on chunkserver

    CSIOMetric ioMetrics_;
    //Self pointing pointer for singleton mode

    static ChunkServerMetric *self_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_
