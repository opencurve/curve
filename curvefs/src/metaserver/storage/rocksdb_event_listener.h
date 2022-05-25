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
 * Project: curve
 * Date: Saturday Jun 04 00:44:34 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_EVENT_LISTENER_H_
#define CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_EVENT_LISTENER_H_

#include <bvar/bvar.h>

#include "rocksdb/db.h"
#include "rocksdb/listener.h"

namespace curvefs {
namespace metaserver {
namespace storage {

// Collect some metrics from rocksdb since start
class MetricEventListener : public rocksdb::EventListener {
 public:
    MetricEventListener();

    void OnFlushBegin(rocksdb::DB* db,
                      const rocksdb::FlushJobInfo& info) override;

    void OnFlushCompleted(rocksdb::DB* db,
                          const rocksdb::FlushJobInfo& info) override;

    void OnCompactionBegin(rocksdb::DB* db,
                           const rocksdb::CompactionJobInfo& info) override;

    void OnCompactionCompleted(rocksdb::DB* db,
                               const rocksdb::CompactionJobInfo& info) override;

    void OnMemTableSealed(const rocksdb::MemTableInfo& info) override;

    void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override;

 private:
    // number of flushing tasks
    bvar::Adder<int64_t> flushing_;
    // flush latency
    bvar::LatencyRecorder flushLatency_;
    // total bytes of flush
    bvar::Adder<uint64_t> flushedBytes_;

    // number of compacting tasks
    bvar::Adder<int64_t> compacting_;
    // compaction latency
    bvar::LatencyRecorder compactionLatency_;

    // total number of sealed memtable
    bvar::Adder<int64_t> sealedMemtable_;

    // total number of delayed write operations
    bvar::Adder<int64_t> delayedWrite_;
    // total number of stopped write operations
    bvar::Adder<int64_t> stoppedWrite_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_EVENT_LISTENER_H_
