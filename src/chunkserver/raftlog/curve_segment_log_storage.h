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
 * Created Date: 2020-09-03
 * Author: charisu
 */

// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_LOG_STORAGE_H_
#define  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_LOG_STORAGE_H_

#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <braft/log_entry.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <map>
#include <vector>
#include <string>
#include <functional>
#include <memory>
#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftlog/segment.h"
#include "src/chunkserver/raftlog/curve_segment.h"
#include "src/chunkserver/raftlog/braft_segment.h"

namespace curve {
namespace chunkserver {

class CurveSegmentLogStorage;

struct LogStorageOptions {
    std::shared_ptr<FilePool> walFilePool;
    std::function<void(CurveSegmentLogStorage*)> monitorMetricCb;

    LogStorageOptions() = default;
    LogStorageOptions(std::shared_ptr<FilePool> walFilePool,
        std::function<void(CurveSegmentLogStorage*)> monitorMetricCb)
        : walFilePool(walFilePool), monitorMetricCb(monitorMetricCb) {
    }
};

struct LogStorageStatus {
    explicit LogStorageStatus(uint32_t walSegmentFileCount)
        : walSegmentFileCount(walSegmentFileCount) {
    }

    uint32_t walSegmentFileCount;
};

LogStorageOptions StoreOptForCurveSegmentLogStorage(
    LogStorageOptions options);

void RegisterCurveSegmentLogStorageOrDie();

// LogStorage use segmented append-only file, all data in disk, all index
// in memory. append one log entry, only cause one disk write, every disk
// write will call fsync().
//
// SegmentLog layout:
//      log_meta: record start_log
//      log_000001-0001000: closed segment
//      log_inprogress_0001001: open segment
class CurveSegmentLogStorage : public braft::LogStorage {
 public:
    typedef std::map<int64_t, scoped_refptr<Segment> > SegmentMap;

    explicit CurveSegmentLogStorage(const std::string& path,
        bool enable_sync = true,
        std::shared_ptr<FilePool> walFilePool = nullptr)
        : _path(path)
        , _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(enable_sync)
        , _walFilePool(walFilePool)
    {}

    CurveSegmentLogStorage()
        : _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(true)
        , _walFilePool(nullptr)
    {}

    virtual ~CurveSegmentLogStorage() {}

    // init logstorage, check consistency and integrity
    virtual int init(braft::ConfigurationManager* configuration_manager);

    // first log index in log
    virtual int64_t first_log_index() {
        return _first_log_index.load(butil::memory_order_acquire);
    }

    // last log index in log
    virtual int64_t last_log_index();

    // get logentry by index
    virtual braft::LogEntry* get_entry(const int64_t index);

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index);

    // append entry to log
    int append_entry(const braft::LogEntry* entry);
    void zyb_test(uint64_t index);

    // append entries to log, return success append number
    virtual int append_entries(const std::vector<braft::LogEntry*>& entries);

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail,
    // (last_index_kept, infinity) will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept);

    virtual int reset(const int64_t next_log_index);

    LogStorage* new_instance(const std::string& uri) const;

    SegmentMap& segments() {
        return _segments;
    }

    void list_files(std::vector<std::string>* seg_files);

    void sync();

    LogStorageStatus GetStatus();

 private:
    scoped_refptr<Segment> open_segment(size_t to_write);
    int save_meta(const int64_t log_index);
    int load_meta();
    int list_segments(bool is_empty);
    int load_segments(braft::ConfigurationManager* configuration_manager);
    int get_segment(int64_t log_index, scoped_refptr<Segment>* ptr);
    void pop_segments(
            int64_t first_index_kept,
            std::vector<scoped_refptr<Segment> >* poped);
    void pop_segments_from_back(
            const int64_t first_index_kept,
            std::vector<scoped_refptr<Segment> >* popped,
            scoped_refptr<Segment>* last_segment);


    std::string _path;
    butil::atomic<int64_t> _first_log_index;
    butil::atomic<int64_t> _last_log_index;
    braft::raft_mutex_t _mutex;
    SegmentMap _segments;
    scoped_refptr<Segment> _open_segment;
    std::shared_ptr<FilePool> _walFilePool;
    int _checksum_type;
    bool _enable_sync;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_LOG_STORAGE_H_
