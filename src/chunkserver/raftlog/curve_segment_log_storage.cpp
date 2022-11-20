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

#include <braft/protobuf_file.h>
#include <braft/local_storage.pb.h>
#include "src/chunkserver/raftlog/curve_segment_log_storage.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftlog/define.h"

namespace curve {
namespace chunkserver {

LogStorageOptions StoreOptForCurveSegmentLogStorage(
    LogStorageOptions options) {
    static LogStorageOptions options_;
    if (nullptr != options.walFilePool) {
        options_ = options;
    }

    return options_;
}

void RegisterCurveSegmentLogStorageOrDie() {
    static CurveSegmentLogStorage logStorage;
    braft::log_storage_extension()->RegisterOrDie(
                                    "curve", &logStorage);
}

int CurveSegmentLogStorage::init(
                    braft::ConfigurationManager* configuration_manager) {
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, braft::FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }

    if (butil::crc32c::IsFastCrc32Supported()) {
        _checksum_type = CHECKSUM_CRC32;
        LOG_ONCE(INFO)
                << "Use crc32c as the checksum type of appending entries";
    } else {
        _checksum_type = CHECKSUM_MURMURHASH32;
        LOG_ONCE(INFO)
                << "Use murmurhash32 as the checksum type of appending entries";
    }

    int ret = 0;
    bool is_empty = false;
    do {
        ret = load_meta();
        if (ret != 0 && errno == ENOENT) {
            LOG(WARNING) << _path << " is empty";
            is_empty = true;
        } else if (ret != 0) {
            break;
        }

        ret = list_segments(is_empty);
        if (ret != 0) {
            break;
        }

        ret = load_segments(configuration_manager);
        if (ret != 0) {
            break;
        }
    } while (0);

    if (is_empty) {
        _first_log_index.store(1);
        _last_log_index.store(0);
        ret = save_meta(1);
    }
    return ret;
}

int CurveSegmentLogStorage::load_meta() {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" BRAFT_SEGMENT_META_FILE);

    braft::ProtoBufFile pb_file(meta_path);
    braft::LogPBMeta meta;
    if (0 != pb_file.load(&meta)) {
        PLOG_IF(ERROR, errno != ENOENT)
                << "Fail to load meta from " << meta_path;
        return -1;
    }

    _first_log_index.store(meta.first_log_index());

    timer.stop();
    LOG(INFO) << "log load_meta " << meta_path
              << " first_log_index: " << meta.first_log_index()
              << " time: " << timer.u_elapsed();
    return 0;
}

int CurveSegmentLogStorage::list_segments(bool is_empty) {
    butil::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION."
                     << " path: " << _path;
        return -1;
    }

    // restore segment meta
    while (dir_reader.Next()) {
        // unlink unneed segments and unfinished unlinked segments
        if ((is_empty && 0 == strncmp(dir_reader.name(),
                                        "log_", strlen("log_"))) ||
            (0 == strncmp(dir_reader.name() +
                        (strlen(dir_reader.name()) - strlen(".tmp")),
                        ".tmp", strlen(".tmp")))) {
            std::string segment_path(_path);
            segment_path.append("/");
            segment_path.append(dir_reader.name());
            ::unlink(segment_path.c_str());

            LOG(WARNING) << "unlink unused segment, path: " << segment_path;

            continue;
        }

        // Is braft log pattern
        int match = 0;
        int64_t first_index = 0;
        int64_t last_index = 0;
        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_CLOSED_PATTERN,
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            BraftSegment* segment = new BraftSegment(_path, first_index,
                                                    last_index, _checksum_type);
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), BRAFT_SEGMENT_OPEN_PATTERN,
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment =
                    new BraftSegment(_path, first_index, _checksum_type);
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " first_index: " << first_index;
                return -1;
            }
        }

        // Is curve log pattern
        match = sscanf(dir_reader.name(), CURVE_SEGMENT_CLOSED_PATTERN,
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            CurveSegment* segment = new CurveSegment(_path, first_index,
                                                     last_index,
                                                     _checksum_type,
                                                     _walFilePool);
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), CURVE_SEGMENT_OPEN_PATTERN,
                       &first_index);
        if (match == 1) {
            BRAFT_VLOG << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment =
                    new CurveSegment(_path, first_index, _checksum_type,
                                     _walFilePool);
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " first_index: " << first_index;
                return -1;
            }
        }
    }

    // check segment
    int64_t last_log_index = -1;
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ) {
        Segment* segment = it->second.get();
        if (segment->first_index() > segment->last_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index != -1 &&
                   segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_log_index: " << last_log_index;
            return -1;
        } else if (last_log_index == -1 &&
                    _first_log_index.load(butil::memory_order_acquire)
                    < segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " first_log_index: "
                << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " first_log_index: "
                << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        ++it;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
            _first_log_index.load(butil::memory_order_relaxed) <
                                        _open_segment->first_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " first_log_index: "
            << _first_log_index.load(butil::memory_order_relaxed)
            << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 &&
                        _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " first_log_index: "
                << _first_log_index.load(butil::memory_order_relaxed)
                << " first_index: " << _open_segment->first_index();
        }
        CHECK_LE(last_log_index, _open_segment->last_index());
    }

    return 0;
}

int CurveSegmentLogStorage::load_segments(
                braft::ConfigurationManager* configuration_manager) {
    int ret = 0;

    // closed segments
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        Segment* segment = it->second.get();
        LOG(INFO) << "load closed segment, path: " << _path
            << " first_index: " << segment->first_index()
            << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        _last_log_index.store(segment->last_index(),
                                    butil::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(INFO) << "load open segment, path: " << _path
            << " first_index: " << _open_segment->first_index();
        ret = _open_segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        if (_first_log_index.load() > _open_segment->last_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load()
                << " first_index: " << _open_segment->first_index()
                << " last_index: " << _open_segment->last_index();
            _open_segment->unlink();
            _open_segment = NULL;
        } else {
            _last_log_index.store(_open_segment->last_index(),
                                 butil::memory_order_release);
        }
        do {
            if (dynamic_cast<BraftSegment *>(_open_segment.get()) != nullptr) {
                LOG(INFO) << "Loaded a braft open segment, close it directly";
                scoped_refptr<Segment> prev_open_segment;
                prev_open_segment.swap(_open_segment);
                _segments[prev_open_segment->first_index()] = prev_open_segment;
                if (prev_open_segment->close(_enable_sync) == 0) {
                    break;
                }
                PLOG(ERROR) << "Fail to close old open_segment path: " << _path;
                // Failed, revert former changes
                BAIDU_SCOPED_LOCK(_mutex);
                _segments.erase(prev_open_segment->first_index());
                _open_segment.swap(prev_open_segment);
                return -1;
            }
        } while (0);
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

int64_t CurveSegmentLogStorage::last_log_index() {
    return _last_log_index.load(butil::memory_order_acquire);
}

braft::LogEntry* CurveSegmentLogStorage::get_entry(const int64_t index) {
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return NULL;
    }
    return ptr->get(index);
}

int CurveSegmentLogStorage::get_segment(int64_t index,
                                        scoped_refptr<Segment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (first_index == last_index + 1) {
        return -1;
    }
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index) << "Attempted to access entry "
            << index << " outside of log, "
            << " first_log_index: " << first_index
            << " last_log_index: " << last_index;
        return -1;
    } else if (index == last_index + 1) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != NULL);
    } else {
        CHECK(!_segments.empty());
        SegmentMap::iterator it = _segments.upper_bound(index);
        SegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

int64_t CurveSegmentLogStorage::get_term(const int64_t index) {
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

int CurveSegmentLogStorage::append_entry(const braft::LogEntry* entry) {
    scoped_refptr<Segment> segment =
                open_segment(entry->data.size() + kEntryHeaderSize);
    if (NULL == segment) {
        return EIO;
    }
    int ret = segment->append(entry);
    if (ret != 0 && ret != EEXIST) {
        return ret;
    }
    if (EEXIST == ret && entry->id.term != get_term(entry->id.index)) {
        return EINVAL;
    }
    _last_log_index.fetch_add(1, butil::memory_order_release);

    return segment->sync(_enable_sync);
}

int CurveSegmentLogStorage::append_entries(
                    const std::vector<braft::LogEntry*>& entries,
                    braft::IOMetric* metric) {
    if (entries.empty()) {
        return 0;
    }
    if (_last_log_index.load(butil::memory_order_relaxed) + 1
            != entries.front()->id.index) {
        LOG(FATAL) << "There's gap between appending entries and"
                   << " _last_log_index path: " << _path;
        return -1;
    }
    scoped_refptr<Segment> last_segment = NULL;
    for (size_t i = 0; i < entries.size(); i++) {
        braft::LogEntry* entry = entries[i];

        scoped_refptr<Segment> segment =
                    open_segment(entry->data.size() + kEntryHeaderSize);
        if (NULL == segment) {
            return i;
        }
        int ret = segment->append(entry);
        if (0 != ret) {
            return i;
        }
        _last_log_index.fetch_add(1, butil::memory_order_release);
        last_segment = segment;
    }
    last_segment->sync(_enable_sync);
    return entries.size();
}

int CurveSegmentLogStorage::truncate_prefix(const int64_t first_index_kept) {
    // segment files
    if (_first_log_index.load(butil::memory_order_acquire) >=
                                                    first_index_kept) {
      BRAFT_VLOG << "Nothing is going to happen since _first_log_index="
                     << _first_log_index.load(butil::memory_order_relaxed)
                     << " >= first_index_kept="
                     << first_index_kept;
        return 0;
    }
    // NOTE: truncate_prefix is not important, as it has nothing to do with
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    std::vector<scoped_refptr<Segment> > popped;
    pop_segments(first_index_kept, &popped);
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

int CurveSegmentLogStorage::save_meta(const int64_t log_index) {
    butil::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" BRAFT_SEGMENT_META_FILE);

    braft::LogPBMeta meta;
    meta.set_first_log_index(log_index);
    braft::ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&meta, braft::raft_sync_meta());

    timer.stop();
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << meta_path;
    LOG(INFO) << "log save_meta " << meta_path << " first_log_index: "
              << log_index << " time: " << timer.u_elapsed();
    return ret;
}

void CurveSegmentLogStorage::pop_segments(
        const int64_t first_index_kept,
        std::vector<scoped_refptr<Segment> >* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, butil::memory_order_release);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        scoped_refptr<Segment>& segment = it->second;
        if (segment->last_index() < first_index_kept) {
            popped->push_back(segment);
            _segments.erase(it++);
        } else {
            return;
        }
    }
    if (_open_segment) {
        if (_open_segment->last_index() < first_index_kept) {
            popped->push_back(_open_segment);
            _open_segment = NULL;
            // _log_storage is empty
            _last_log_index.store(first_index_kept - 1);
        } else {
            CHECK(_open_segment->first_index() <= first_index_kept);
        }
    } else {
        // _log_storage is empty
        _last_log_index.store(first_index_kept - 1);
    }
}

void CurveSegmentLogStorage::pop_segments_from_back(
        const int64_t last_index_kept,
        std::vector<scoped_refptr<Segment> >* popped,
        scoped_refptr<Segment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    *last_segment = NULL;
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, butil::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment = NULL;
    }
    for (SegmentMap::reverse_iterator
            it = _segments.rbegin(); it != _segments.rend(); ++it) {
        if (it->second->first_index() <= last_index_kept) {
            // Not return as we need to maintain _segments at the end of this
            // routine
            break;
        }
        popped->push_back(it->second);
        // XXX: C++03 not support erase reverse_iterator
    }
    for (size_t i = 0; i < popped->size(); i++) {
        _segments.erase((*popped)[i]->first_index());
    }
    if (_segments.rbegin() != _segments.rend()) {
        *last_segment = _segments.rbegin()->second;
    } else {
        // all the logs have been cleared, the we move _first_log_index to the
        // next index
        _first_log_index.store(last_index_kept + 1,
                                butil::memory_order_release);
    }
}

int CurveSegmentLogStorage::truncate_suffix(const int64_t last_index_kept) {
    // segment files
    std::vector<scoped_refptr<Segment> > popped;
    scoped_refptr<Segment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    bool truncate_last_segment = false;
    int ret = -1;

    if (last_segment) {
        if (_first_log_index.load(butil::memory_order_relaxed) <=
            _last_log_index.load(butil::memory_order_relaxed)) {
            truncate_last_segment = true;
        } else {
            // trucate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK(_open_segment.get() == last_segment.get());
                _open_segment = NULL;
            }
        }
    }

    // The truncate suffix order is crucial to satisfy log matching
    // property of raft log must be truncated from back to front.
    for (size_t i = 0; i < popped.size(); ++i) {
        ret = popped[i]->unlink();
        if (ret != 0) {
            return ret;
        }
        popped[i] = NULL;
    }
    if (truncate_last_segment) {
        bool closed = !last_segment->is_open();
        ret = last_segment->truncate(last_index_kept);
        if (ret == 0 && closed && last_segment->is_open()) {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK(!_open_segment);
            _open_segment.swap(last_segment);
        }
    }

    return ret;
}

int CurveSegmentLogStorage::reset(const int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index
                   << " path: " << _path;
        return EINVAL;
    }
    std::vector<scoped_refptr<Segment> > popped;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    popped.reserve(_segments.size());
    for (SegmentMap::const_iterator
            it = _segments.begin(); it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }
    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment = NULL;
    }
    _first_log_index.store(next_log_index, butil::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, butil::memory_order_relaxed);
    lck.unlock();
    // NOTE: see the comments in truncate_prefix
    if (save_meta(next_log_index) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        return -1;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
    return 0;
}

void CurveSegmentLogStorage::list_files(std::vector<std::string>* seg_files) {
    BAIDU_SCOPED_LOCK(_mutex);
    seg_files->push_back(BRAFT_SEGMENT_META_FILE);
    for (SegmentMap::iterator it = _segments.begin();
                                it != _segments.end(); ++it) {
        scoped_refptr<Segment>& segment = it->second;
        seg_files->push_back(segment->file_name());
    }
    if (_open_segment) {
        seg_files->push_back(_open_segment->file_name());
    }
}

void CurveSegmentLogStorage::sync() {
    std::vector<scoped_refptr<Segment> > segments;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        for (SegmentMap::iterator it = _segments.begin();
                                it != _segments.end(); ++it) {
            segments.push_back(it->second);
        }
    }

    for (size_t i = 0; i < segments.size(); i++) {
        segments[i]->sync(true);
    }
}

braft::LogStorage* CurveSegmentLogStorage::new_instance(
    const std::string& uri) const {
    LogStorageOptions options = StoreOptForCurveSegmentLogStorage(
        LogStorageOptions());

    CHECK(nullptr != options.walFilePool) << "wal file pool is null";

    CurveSegmentLogStorage* logStorage = new CurveSegmentLogStorage(
        uri, true, options.walFilePool);
    options.monitorMetricCb(logStorage);

    return logStorage;
}

scoped_refptr<Segment> CurveSegmentLogStorage::open_segment(
                                                    size_t to_write) {
    scoped_refptr<Segment> prev_open_segment;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment = new CurveSegment(_path, last_log_index() + 1,
                                             _checksum_type, _walFilePool);
            if (_open_segment->create() != 0) {
                _open_segment = NULL;
                return NULL;
            }
        }
        uint32_t maxTotalFileSize = _walFilePool->GetFilePoolOpt().fileSize
                                  + _walFilePool->GetFilePoolOpt().metaPageSize;
        if (_open_segment->bytes() + to_write > maxTotalFileSize) {
            _segments[_open_segment->first_index()] = _open_segment;
            prev_open_segment.swap(_open_segment);
        }
    }
    do {
        if (prev_open_segment) {
            if (prev_open_segment->close(_enable_sync) == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment = new CurveSegment(_path, last_log_index() + 1,
                                                 _checksum_type, _walFilePool);
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            PLOG(ERROR) << "Fail to close old open_segment or create new"
                        << " open_segment path: " << _path;
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
            return NULL;
        }
    } while (0);
    return _open_segment;
}

LogStorageStatus CurveSegmentLogStorage::GetStatus() {
    uint32_t count = (uint32_t)(_segments.size())
                   + (nullptr != _open_segment ? 1 : 0);
    return LogStorageStatus(count);
}

}  // namespace chunkserver
}  // namespace curve
