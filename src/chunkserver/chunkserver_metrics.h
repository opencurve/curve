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

using curve::common::Uncopyable;
using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::Configuration;

namespace curve {
namespace chunkserver {

class CopysetNodeManager;
class FilePool;
class CSDataStore;
class Trash;

template <typename Tp>
using PassiveStatusPtr = std::shared_ptr<bvar::PassiveStatus<Tp>>;

template <typename Tp>
using AdderPtr = std::shared_ptr<bvar::Adder<Tp>>;

// Use the LatencyRecorder implementation to count the size of read and write
// requests.
// Quantile, maximum, median, mean, etc. can be counted.
using IOSizeRecorder = bvar::LatencyRecorder;

// io related statistics
class IOMetric {
 public:
    IOMetric();
    virtual ~IOMetric();
    /**
     * initialize io metric
     * Mainly used to expose the various metric indicators
     * @param prefix: Prefix used for bvar exposure
     * @return Return 0 for success, -1 for failure
     */
    int Init(const std::string& prefix);
    /**
     * Count requestNum when IO requests arrive
     */
    void OnRequest();
    /**
     * After the IO is completed, record the metrics of the IO
     * Wrong io is not counted into iops and bps statistics
     * @param size: Size of this io
     * @param latUS: Latency of this io
     * @param hasError: Whether there are any errors generated by this io
     */
    void OnResponse(size_t size, int64_t latUs, bool hasError);

 public:
    // Number of io requests
    bvar::Adder<uint64_t>    reqNum_;
    // Number of successful io
    bvar::Adder<uint64_t>    ioNum_;
    // Number of error io
    bvar::Adder<uint64_t>    errorNum_;
    // Bytes of io
    bvar::Adder<uint64_t>    ioBytes_;
    // latency of io（quantile, maximum, median, mean）
    bvar::LatencyRecorder    latencyRecorder_;
    // Size of io（quantile, maximum, median, mean）
    IOSizeRecorder           sizeRecorder_;
    // rps in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>>    rps_;
    // iops in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>>    iops_;
    // eps in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>>    eps_;
    // bps in the last 1 second
    bvar::PerSecond<bvar::Adder<uint64_t>>    bps_;
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
        : readMetric_(nullptr)
        , writeMetric_(nullptr)
        , recoverMetric_(nullptr)
        , pasteMetric_(nullptr)
        , downloadMetric_(nullptr) {}

    ~CSIOMetric() {}

    /**
     * Record metric before executing request
     * @param type: The corresponding metric type
     */
    void OnRequest(CSIOMetricType type);

    /**
     * After the IO is completed, record the metrics of the IO
     * Wrong io is not counted into iops and bps statistics
     * @param size: Size of this io
     * @param latUS: Latency of this io
     * @param hasError: Whether there are any errors generated by this io
     */
    void OnResponse(CSIOMetricType type,
                    size_t size,
                    int64_t latUs,
                    bool hasError);

    /**
     * Get the specified type of IOMetric
     * @param type: Corresponding metric type
     * @return Return the IOMetric pointer of the specified type, or nullptr
     * if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type);

    /**
     * Initialize the metric statistics for each op
     * @return Return 0 for success, -1 for failure
     */
    int Init(const std::string& prefix);
    /**
     * Release the metric resources of each op
     */
    void Fini();

 protected:
    // ReadChunk statistics
    IOMetricPtr readMetric_;
    // WriteChunk statistics
    IOMetricPtr writeMetric_;
    // RecoverChunk statistics
    IOMetricPtr recoverMetric_;
    // PasteChunk infomation
    IOMetricPtr pasteMetric_;
    // Download statistics
    IOMetricPtr downloadMetric_;
};

class CSCopysetMetric {
 public:
    CSCopysetMetric()
        : logicPoolId_(0)
        , copysetId_(0)
        , chunkCount_(nullptr)
        , snapshotCount_(nullptr)
        , cloneChunkCount_(nullptr) {}

    ~CSCopysetMetric() {}

    /**
     * Initialize metric statistics at copyset level
     * @param logicPoolId: The id of the logical pool to which copyset belongs
     * @param copysetId: Copyset id
     * @return Return 0 for success, -1 for failure
     */
    int Init(const LogicPoolID& logicPoolId, const CopysetID& copysetId);

    /**
     * Monitor DataStore metrics, mainly the number of chunks, snapshots, etc.
     * @param datastore: Pointer to the datastore under this copyset
     */
    void MonitorDataStore(CSDataStore* datastore);

    /**
     * Record metric before executing request
     * @param type: Corresponding metric types
     */
    void OnRequest(CSIOMetricType type) {
        ioMetrics_.OnRequest(type);
    }

    /**
     * After the IO is completed, record the metrics of the IO
     * Wrong io is not counted into iops and bps statistics
     * @param size: Size of this io
     * @param latUS: Latency of this io
     * @param hasError: Whether there are any errors generated by this io
     */
    void OnResponse(CSIOMetricType type,
                    size_t size,
                    int64_t latUs,
                    bool hasError) {
        ioMetrics_.OnResponse(type, size, latUs, hasError);
    }

    /**
     * Get the specified type of IOMetric
     * @param type: Corresponding metric types
     * @return Return the IOMetric pointer of the specified type, or nullptr
     *         if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type) {
        return ioMetrics_.GetIOMetric(type);
    }

    const uint32_t GetChunkCount() const {
        if (chunkCount_ == nullptr) {
            return 0;
        }
        return chunkCount_->get_value();
    }

    const uint32_t GetSnapshotCount() const {
        if (snapshotCount_ == nullptr) {
            return 0;
        }
        return snapshotCount_->get_value();
    }

    const uint32_t GetCloneChunkCount() const {
        if (cloneChunkCount_ == nullptr) {
            return 0;
        }
        return cloneChunkCount_->get_value();
    }

 private:
    inline std::string Prefix() {
        return "copyset_"
               + std::to_string(logicPoolId_)
               + "_"
               + std::to_string(copysetId_);
    }

 private:
    // logicPool id
    LogicPoolID logicPoolId_;
    // copyset id
    CopysetID copysetId_;
    // Number of chunks on copyset
    PassiveStatusPtr<uint32_t> chunkCount_;
    // Number of snapshots on copyset
    PassiveStatusPtr<uint32_t> snapshotCount_;
    // Number of clone chunks on copyset
    PassiveStatusPtr<uint32_t> cloneChunkCount_;
    // Metric statistics for IO types on copyset
    CSIOMetric ioMetrics_;
};

struct ChunkServerMetricOptions {
    bool collectMetric;
    // chunkserve ip
    std::string ip;
    // chunkserver port
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
    // Protect read-write locks on copyset metric maps
    RWLock rwLock_;
    // Map for each copyset metric, using GroupId as key
    std::unordered_map<GroupId, CopysetMetricPtr> map_;
};

class ChunkServerMetric : public Uncopyable {
 public:
    // Implementing the singleton
    static ChunkServerMetric *GetInstance();

    /**
     * Initialize chunkserver statistics
     * @pa)ram option: Initialize configurations
     * @return Return 0 for success, -1 for failure
     */
    int Init(const ChunkServerMetricOptions& option);

    /**
     * Free metric resources
     * @return Return 0 for success, -1 for failure
     */
    int Fini();

    /**
     * Record metric before request
     * @param logicPoolId:The logical pool id where this io operation is located
     * @param copysetId: The copysetid where this io operation is located
     * @param type: Request type
     */
    void OnRequest(const LogicPoolID& logicPoolId,
                   const CopysetID& copysetId,
                   CSIOMetricType type);
    /**
    * After the IO is completed, record the metrics of the IO
    * Wrong io is not counted into iops and bps statistics
    * @param logicPoolId: The logical pool id where this io operation is located
    * @param copysetId: The copyset id where this io operation is located
    * @param type: Request type
    * @param size: Size of this io
    * @param latUS: Latency of this io
    * @param hasError: Whether there are any errors generated by this io
    */
    void OnResponse(const LogicPoolID& logicPoolId,
                    const CopysetID& copysetId,
                    CSIOMetricType type,
                    size_t size,
                    int64_t latUs,
                    bool hasError);

    /**
     * Create the metric of the specified copyset
     * If collectMetric is false, it returns 0, but does not actually create
     * @param logicPoolId: The id of the logical pool to which copyset belongs
     * @param copysetId: copyset id
     * @return Return 0 for success, -1 for failure, or failure if the
     * specified metric already exists
     */
    int CreateCopysetMetric(const LogicPoolID& logicPoolId,
                            const CopysetID& copysetId);

    /**
     * Get the metric of the specified copyset
     * @param logicPoolId: The id of the logical pool to which copyset belongs
     * @param copysetId: copyset id
     * @return Return the specified copyset metric for success, nullptr for
     * failure
     */
    CopysetMetricPtr GetCopysetMetric(const LogicPoolID& logicPoolId,
                                      const CopysetID& copysetId);

    /**
     * Delete the metric of the specified copyset
     * @param logicPoolId: The id of the logical pool to which copyset belongs
     * @param copysetId: copyset id
     * @return Return 0 for success, -1 for failure
     */
    int RemoveCopysetMetric(const LogicPoolID& logicPoolId,
                            const CopysetID& copysetId);

    /**
     * Monitor the chunk allocation pool, mainly monitor the number of chunks
     * in the pool
     * @param chunkFilePool: Object pointer to chunkfilePool
     */
    void MonitorChunkFilePool(FilePool* chunkFilePool);

    /**
     * Monitor the wal segment allocation pool, mainly monitor the number of
     * segments in the pool
     * @param walFilePool: Object pointer to walfilePool
     */
    void MonitorWalFilePool(FilePool* walFilePool);

    /**
     * Monitor the trash
     * @param trash: Object pointer to trash
     */
    void MonitorTrash(Trash* trash);

    /**
     * Increase leader count
     */
    void IncreaseLeaderCount();

    /**
     * Decrease leader count
     */
    void DecreaseLeaderCount();

    /**
     * Update configuration data
     * @param conf: Configuration contents
     */
    void ExposeConfigMetric(common::Configuration* conf);

    /**
     * Get the specified type of IOMetric
     * @param type: The corresponding metric type
     * @return Return the IOMetric pointer of the specified type, or nullptr
     * if the type does not exist
     */
    IOMetricPtr GetIOMetric(CSIOMetricType type) {
        return ioMetrics_.GetIOMetric(type);
    }

    CopysetMetricMap* GetCopysetMetricMap() {
        return &copysetMetricMap_;
    }

    const uint32_t GetCopysetCount() {
        return copysetMetricMap_.Size();
    }

    const uint32_t GetLeaderCount() const {
        if (leaderCount_ == nullptr)
            return 0;
        return leaderCount_->get_value();
    }

    const uint32_t GetTotalChunkCount() {
        if (chunkCount_ == nullptr)
            return 0;
        return chunkCount_->get_value();
    }

    const uint32_t GetTotalSnapshotCount() {
        if (snapshotCount_ == nullptr)
            return 0;
        return snapshotCount_->get_value();
    }

    const uint32_t GetTotalCloneChunkCount() {
        if (cloneChunkCount_ == nullptr)
            return 0;
        return cloneChunkCount_->get_value();
    }

    const uint32_t GetChunkLeftCount() const {
        if (chunkLeft_ == nullptr)
            return 0;
        return chunkLeft_->get_value();
    }

    const uint32_t GetChunkTrashedCount() const {
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
    // Initialization flags
    bool hasInited_;
    // Configurations
    ChunkServerMetricOptions option_;
    // Number of leader
    AdderPtr<uint32_t> leaderCount_;
    // Number of chunks left in the chunkfilepool
    PassiveStatusPtr<uint32_t> chunkLeft_;
    // Number of remaining wal segments in the walfilepool
    PassiveStatusPtr<uint32_t> walSegmentLeft_;
    // Number of chunks in the trash
    PassiveStatusPtr<uint32_t> chunkTrashed_;
    // Number of chunks in the chunkserver
    PassiveStatusPtr<uint32_t> chunkCount_;
    // Number of snapshot files on chunkserver
    PassiveStatusPtr<uint32_t> snapshotCount_;
    // Number of clone chunk on chunkserver
    PassiveStatusPtr<uint32_t> cloneChunkCount_;
    // Mapping table for each copyset metric, using GroupId as key
    CopysetMetricMap copysetMetricMap_;
    // Metric statistics for IO types on chunkserver
    CSIOMetric ioMetrics_;
    // Self-referencing pointer for singleton mode
    static ChunkServerMetric* self_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_
