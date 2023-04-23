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
 * Date: Saturday Jun 04 00:49:55 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/storage/rocksdb_event_listener.h"

#include <butil/time.h>

namespace curvefs {
namespace metaserver {
namespace storage {

namespace {
thread_local uint64_t rocksdbFlushStart;
thread_local uint64_t rocksdbCompactionStart;
}  // namespace

MetricEventListener::MetricEventListener()
    : flushing_("rocksdb_flushing"),
      flushLatency_("rocksdb_flush"),
      compacting_("rocksdb_compacting"),
      compactionLatency_("rocksdb_compaction"),
      sealedMemtable_("rocksdb_sealed_memtable"),
      delayedWrite_("rocksdb_delayed_write"),
      stoppedWrite_("rocksdb_stopped_write") {}

void MetricEventListener::OnFlushBegin(rocksdb::DB* db,
                                       const rocksdb::FlushJobInfo& /*info*/) {
    (void)db;
    flushing_ << 1;
    rocksdbFlushStart = butil::cpuwide_time_us();
}

void MetricEventListener::OnFlushCompleted(
    rocksdb::DB* db,
    const rocksdb::FlushJobInfo& info) {
    (void)db;
    flushing_ << -1;
    flushLatency_ << (butil::cpuwide_time_us() - rocksdbFlushStart);
    flushedBytes_ << info.table_properties.data_size;
}

void MetricEventListener::OnMemTableSealed(
    const rocksdb::MemTableInfo& /*info*/) {
    sealedMemtable_ << 1;
}

void MetricEventListener::OnCompactionBegin(
    rocksdb::DB* db,
    const rocksdb::CompactionJobInfo& /*info*/) {
    (void)db;
    compacting_ << 1;
    rocksdbCompactionStart = butil::cpuwide_time_us();
}

void MetricEventListener::OnCompactionCompleted(
    rocksdb::DB* db,
    const rocksdb::CompactionJobInfo& /*info*/) {
    (void)db;
    compacting_ << -1;
    compactionLatency_ << (butil::cpuwide_time_us() - rocksdbCompactionStart);
}

void MetricEventListener::OnStallConditionsChanged(
    const rocksdb::WriteStallInfo& info) {
    switch (info.condition.cur) {
        case rocksdb::WriteStallCondition::kNormal:
            return;
        case rocksdb::WriteStallCondition::kDelayed:
            delayedWrite_ << 1;
            return;
        case rocksdb::WriteStallCondition::kStopped:
            stoppedWrite_ << 1;
    }
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
