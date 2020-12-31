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
 * Created Date: 2020-09-02
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

#ifndef  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_
#define  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_

#include <butil/memory/ref_counted.h>
#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <braft/log_entry.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <vector>
#include <memory>
#include <algorithm>
#include <utility>
#include <string>
#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftlog/segment.h"

namespace curve {
namespace chunkserver {

DECLARE_bool(enableWalDirectWrite);

extern std::shared_ptr<FilePool> kWalFilePool;

struct CurveSegmentMeta {
    CurveSegmentMeta() : bytes(0) {}
    int64_t bytes;
};

class BAIDU_CACHELINE_ALIGNMENT CurveSegment:
          public Segment {
 public:
    CurveSegment(const std::string& path, const int64_t first_index,
                 int checksum_type)
        : _path(path), _meta(CurveSegmentMeta()),
        _fd(-1), _direct_fd(-1), _is_open(true),
        _first_index(first_index), _last_index(first_index - 1),
        _checksum_type(checksum_type),
        _meta_page_size(kWalFilePool->GetFilePoolOpt().metaPageSize) {}
    CurveSegment(const std::string& path, const int64_t first_index,
            const int64_t last_index, int checksum_type)
        : _path(path), _meta(CurveSegmentMeta()),
        _fd(-1), _direct_fd(-1), _is_open(false),
        _first_index(first_index), _last_index(last_index),
        _checksum_type(checksum_type),
        _meta_page_size(kWalFilePool->GetFilePoolOpt().metaPageSize) {
    }
    ~CurveSegment() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
        if (_direct_fd >= 0) {
            ::close(_direct_fd);
            _direct_fd = -1;
        }
    }

    struct EntryHeader;

    // create open segment
    int create() override;

    // load open or closed segment
    // open fd, load index, truncate uncompleted entry
    int load(braft::ConfigurationManager* configuration_manager) override;

    // serialize entry, and append to open segment
    int append(const braft::LogEntry* entry) override;

    // get entry by index
    braft::LogEntry* get(const int64_t index) const override;

    // get entry's term by index
    int64_t get_term(const int64_t index) const override;

    // close open segment
    int close(bool will_sync = true) override;

    // sync open segment
    int sync(bool will_sync) override;

    // unlink segment
    int unlink() override;

    // truncate segment to last_index_kept
    int truncate(const int64_t last_index_kept) override;

    bool is_open() const override {
        return _is_open;
    }

    int64_t bytes() const override {
        return _meta.bytes;
    }

    int64_t first_index() const override {
        return _first_index;
    }

    int64_t last_index() const override {
        return _last_index.load(butil::memory_order_consume);
    }

    std::string file_name() override;
 private:
    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    int _load_entry(off_t offset, EntryHeader *head, butil::IOBuf *body,
                    size_t size_hint) const;

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _load_meta();

    int _update_meta_page();

    std::string _path;
    CurveSegmentMeta _meta;
    mutable braft::raft_mutex_t _mutex;
    int _fd;
    int _direct_fd;
    bool _is_open;
    const int64_t _first_index;
    butil::atomic<int64_t> _last_index;
    int _checksum_type;
    std::vector<std::pair<int64_t, int64_t> > _offset_and_term;
    uint32_t _meta_page_size;

    std::vector<std::pair<char*, size_t>> _io_vec;
    std::vector<int64_t> _term_to_add;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_
