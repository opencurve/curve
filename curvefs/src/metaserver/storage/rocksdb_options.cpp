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
 * Date: Thursday Jun 09 10:34:12 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/storage/rocksdb_options.h"

#include <gflags/gflags.h>

#include <memory>
#include <mutex>

#include "curvefs/src/metaserver/storage/rocksdb_event_listener.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "src/common/gflags_helper.h"

namespace curvefs {
namespace metaserver {
namespace storage {

// TODO(wuhanqing): block cache and writer buffer manager's capacity should be
//                  related to the number of copysets
DEFINE_int64(rocksdb_block_cache_capacity,
             8ULL << 30,
             "Total block cache capacity for all rocksdb instances");

DEFINE_int64(rocksdb_write_buffer_manager_capacity,
             6ULL << 30,
             "Total writer buffer capacity for all rocksdb instances");

// https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager
DEFINE_bool(rocksdb_WBM_cost_block_cache,
            true,
            "Control wether write buffer manager cost block cache");

DEFINE_int32(
    rocksdb_max_background_jobs,
    16,
    "Maximum number of concurrent background jobs (compactions and flushes)");

DEFINE_double(rocksdb_memtable_prefix_bloom_size_ratio,
              0.1,
              "Rocksdb memtable prefix bloom size ratio");

DEFINE_int64(rocksdb_unordered_cf_write_buffer_size,
             64 << 20,
             "Writer buffer size for unordered column family");

DEFINE_int32(rocksdb_unordered_cf_max_write_buffer_number,
             2,
             "Number of writer buffer for unordered column family");

DEFINE_int64(rocksdb_ordered_cf_write_buffer_size,
             64 << 20,
             "Writer buffer size for ordered column family");

DEFINE_int32(rocksdb_ordered_cf_max_write_buffer_number,
             2,
             "Number of writer buffer for ordered column family");

DEFINE_int32(rocksdb_stats_dump_period_sec,
             180,
             "Dump rocksdb.stats to LOG every stats_dump_period_sec");

namespace {

std::shared_ptr<rocksdb::Cache> rocksdbBlockCache;
std::shared_ptr<rocksdb::WriteBufferManager> rocksdbWriteBufferManager;
std::shared_ptr<MetricEventListener> metricEventListener;

const char* const kOrderedColumnFamilyName = "ordered_column_family";

void CreateBlockCacheAndWriterBufferManager() {
    static std::once_flag createBlockCache;
    std::call_once(createBlockCache, []() {
        rocksdbBlockCache = rocksdb::NewLRUCache(
            FLAGS_rocksdb_block_cache_capacity, 1, false, 0.9);
    });

    static std::once_flag createWriterBufferManager;
    std::call_once(createWriterBufferManager, []() {
        rocksdbWriteBufferManager =
            std::make_shared<rocksdb::WriteBufferManager>(
                FLAGS_rocksdb_write_buffer_manager_capacity,
                FLAGS_rocksdb_WBM_cost_block_cache ? rocksdbBlockCache
                                                   : nullptr);
    });
}

std::shared_ptr<MetricEventListener> GetMetricEventListener() {
    static std::once_flag once;
    std::call_once(once, []() {
        metricEventListener = std::make_shared<MetricEventListener>();
    });

    return metricEventListener;
}

}  // namespace

void InitRocksdbOptions(
    rocksdb::DBOptions* options,
    std::vector<rocksdb::ColumnFamilyDescriptor>* columnFamilies) {
    assert(options != nullptr);
    assert(columnFamilies != nullptr);
    columnFamilies->clear();

    CreateBlockCacheAndWriterBufferManager();

    options->create_if_missing = true;
    options->create_missing_column_families = true;
    options->max_background_jobs = FLAGS_rocksdb_max_background_jobs;
    options->atomic_flush = true;
    options->bytes_per_sync = 1ULL << 20;  // 1MiB
    options->write_buffer_manager = rocksdbWriteBufferManager;
    options->info_log_level = rocksdb::INFO_LEVEL;
    options->listeners.push_back(GetMetricEventListener());
    options->statistics = rocksdb::CreateDBStatistics();
    options->stats_dump_period_sec = FLAGS_rocksdb_stats_dump_period_sec;

    rocksdb::BlockBasedTableOptions tableOptions;
    tableOptions.block_size = 16ULL << 10;  // 16KiB
    tableOptions.block_cache = rocksdbBlockCache;
    tableOptions.cache_index_and_filter_blocks = true;
    tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;
    tableOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

    rocksdb::ColumnFamilyOptions defaultCfOptions;
    defaultCfOptions.enable_blob_files = true;
    defaultCfOptions.level_compaction_dynamic_level_bytes = true;
    defaultCfOptions.compaction_pri = rocksdb::kMinOverlappingRatio;
    defaultCfOptions.prefix_extractor.reset(
        rocksdb::NewFixedPrefixTransform(RocksDBStorage::GetKeyPrefixLength()));
    defaultCfOptions.memtable_prefix_bloom_size_ratio =
        FLAGS_rocksdb_memtable_prefix_bloom_size_ratio;
    defaultCfOptions.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(tableOptions));

    rocksdb::ColumnFamilyOptions orderedCfOptions = defaultCfOptions;
    orderedCfOptions.write_buffer_size =
        FLAGS_rocksdb_ordered_cf_write_buffer_size;
    orderedCfOptions.max_write_buffer_number =
        FLAGS_rocksdb_ordered_cf_max_write_buffer_number;

    rocksdb::ColumnFamilyOptions unorderedCfOptions = defaultCfOptions;
    unorderedCfOptions.write_buffer_size =
        FLAGS_rocksdb_unordered_cf_write_buffer_size;
    unorderedCfOptions.max_write_buffer_number =
        FLAGS_rocksdb_unordered_cf_max_write_buffer_number;

    columnFamilies->push_back(rocksdb::ColumnFamilyDescriptor{
        rocksdb::kDefaultColumnFamilyName, unorderedCfOptions});
    columnFamilies->push_back(rocksdb::ColumnFamilyDescriptor{
        kOrderedColumnFamilyName, orderedCfOptions});
}

void ParseRocksdbOptions(curve::common::Configuration* conf) {
    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(conf, "rocksdb_block_cache_capacity",
               "storage.rocksdb.block_cache_capacity",
               &FLAGS_rocksdb_block_cache_capacity, /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_write_buffer_manager_capacity",
               "storage.rocksdb.write_buffer_manager_capacity",
               &FLAGS_rocksdb_write_buffer_manager_capacity,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_WBM_cost_block_cache",
               "storage.rocksdb.WBM_cost_block_cache",
               &FLAGS_rocksdb_WBM_cost_block_cache, /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_max_background_jobs",
               "storage.rocksdb.max_background_jobs",
               &FLAGS_rocksdb_max_background_jobs, /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_memtable_prefix_bloom_size_ratio",
               "storage.rocksdb.memtable_prefix_bloom_size_ratio",
               &FLAGS_rocksdb_memtable_prefix_bloom_size_ratio,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_unordered_cf_write_buffer_size",
               "storage.rocksdb.unordered_write_buffer_size",
               &FLAGS_rocksdb_unordered_cf_write_buffer_size,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_unordered_cf_max_write_buffer_number",
               "storage.rocksdb.unordered_max_write_buffer_number",
               &FLAGS_rocksdb_unordered_cf_max_write_buffer_number,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_ordered_cf_write_buffer_size",
               "storage.rocksdb.ordered_write_buffer_size",
               &FLAGS_rocksdb_ordered_cf_write_buffer_size,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_ordered_cf_max_write_buffer_number",
               "storage.rocksdb.ordered_max_write_buffer_number",
               &FLAGS_rocksdb_ordered_cf_max_write_buffer_number,
               /*fatalIfMissing*/ false);
    dummy.Load(conf, "rocksdb_stats_dump_period_sec",
               "storage.rocksdb.stats_dump_period_sec",
               &FLAGS_rocksdb_stats_dump_period_sec, /*fatalIfMissing*/ false);
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
