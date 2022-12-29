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

#include <fcntl.h>
#include <butil/fd_utility.h>
#include <butil/raw_pack.h>
#include <braft/local_storage.pb.h>
#include <braft/fsync.h>
#include "src/chunkserver/raftlog/curve_segment.h"
#include "src/chunkserver/raftlog/define.h"

namespace curve {
namespace chunkserver {

DEFINE_bool(raftSyncSegments, true, "call fsync when a segment is closed");
DEFINE_bool(enableWalDirectWrite, true, "enable wal direct write or not");
DEFINE_uint32(walAlignSize, 4096, "wal align size to write");

int CurveSegment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on a closed segment at first_index="
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, _first_index);
    char* metaPage = new char[_meta_page_size];
    memset(metaPage, 0, _meta_page_size);
    memcpy(metaPage, &_meta.bytes, sizeof(_meta.bytes));
    int res = _walFilePool->GetFile(path, metaPage);
    delete metaPage;
    if (res != 0) {
        LOG(ERROR) << "Get segment from chunk file pool fail!";
        return -1;
    }
    _fd = ::open(path.c_str(), O_RDWR|O_NOATIME, 0644);
    if (_fd >= 0) {
        butil::make_close_on_exec(_fd);
    } else {
        LOG(ERROR) << "Open path: " << path << " fail, error: "
                   << strerror(errno);
        return -1;
    }
    res = ::lseek(_fd, _meta_page_size, SEEK_SET);
    if (res != _meta_page_size) {
        LOG(ERROR) << "lseek fail! error: " << strerror(errno);
        return -1;
    }
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path
                           << "' with fd=" << _fd;
    if (FLAGS_enableWalDirectWrite) {
        _direct_fd = ::open(path.c_str(), O_RDWR|O_NOATIME|O_DIRECT, 0644);
        LOG_IF(FATAL, _direct_fd < 0) << "failed to open file with O_DIRECT"
                                         ", error: " << strerror(errno);
        butil::make_close_on_exec(_direct_fd);
    }
    _meta.bytes += _meta_page_size;
    _update_meta_page();
    return _fd >= 0 ? 0 : -1;
}

struct CurveSegment::EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_real_len;
    uint32_t data_checksum;
};

std::ostream& operator<<(std::ostream& os,
                         const CurveSegment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", data_real_len=" << h.data_real_len
       << ", checksum_type=" << h.checksum_type << ", data_checksum="
       << h.data_checksum << '}';
    return os;
}

int CurveSegment::load(braft::ConfigurationManager* configuration_manager) {
    int ret = 0;

    std::string path(_path);
    if (_is_open) {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                              _first_index);
    } else {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());
    }
    _fd = ::open(path.c_str(), O_RDWR|O_NOATIME);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }
    butil::make_close_on_exec(_fd);
    if (FLAGS_enableWalDirectWrite) {
        _direct_fd = ::open(path.c_str(), O_RDWR|O_NOATIME|O_DIRECT);
        LOG_IF(FATAL, _direct_fd < 0) << "failed to open file with O_DIRECT";
        butil::make_close_on_exec(_direct_fd);
    }

    // load meta page
    if (_load_meta() != 0) {
        LOG(ERROR) << "Load wal meta page fail";
        return -1;
    }

    // load entry index
    int64_t load_size = _meta.bytes;
    int64_t entry_off = _meta_page_size;
    int64_t actual_last_index = _first_index - 1;
    for (int64_t i = _first_index; entry_off < load_size; i++) {
        EntryHeader header;
        size_t header_size = kEntryHeaderSize;
        const int rc = _load_entry(entry_off, &header, NULL, header_size);
        if (rc > 0) {
            // The last log was not completely written,
            // which should be truncated
            break;
        }
        if (rc < 0) {
            ret = rc;
            break;
        }
        // rc == 0
        const int64_t skip_len = header_size + header.data_len;
        if (entry_off + skip_len > load_size) {
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == braft::ENTRY_TYPE_CONFIGURATION) {
            butil::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, &data,
                            header_size + header.data_real_len) != 0) {
                break;
            }
            scoped_refptr<braft::LogEntry> entry = new braft::LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            butil::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                braft::ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry);
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: "
                           << _path << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(butil::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    } else {
        _offset_and_term.shrink_to_fit();
    }

    // seek to end, for opening segment
    ::lseek(_fd, entry_off, SEEK_SET);

    _meta.bytes = entry_off;
    return ret;
}

int CurveSegment::_load_meta() {
    char* metaPage = new char[_meta_page_size];
    int res = ::pread(_fd, metaPage, _meta_page_size, 0);
    if (res != _meta_page_size) {
        delete metaPage;
        return -1;
    }
    memcpy(&_meta.bytes, metaPage, sizeof(_meta.bytes));
    delete metaPage;
    LOG(INFO) << "loaded bytes: " << _meta.bytes;
    return 0;
}

inline bool verify_checksum(int checksum_type,
                            const char* data, size_t len, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == braft::murmurhash32(data, len));
    case CHECKSUM_CRC32:
        return (value == braft::crc32(data, len));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline bool verify_checksum(int checksum_type,
                            const butil::IOBuf& data, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == braft::murmurhash32(data));
    case CHECKSUM_CRC32:
        return (value == braft::crc32(data));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline uint32_t get_checksum(int checksum_type, const char* data, size_t len) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return braft::murmurhash32(data, len);
    case CHECKSUM_CRC32:
        return braft::crc32(data, len);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

inline uint32_t get_checksum(int checksum_type, const butil::IOBuf& data) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return braft::murmurhash32(data);
    case CHECKSUM_CRC32:
        return braft::crc32(data);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

std::string CurveSegment::file_name() {
    if (!_is_open) {
        return butil::string_printf(CURVE_SEGMENT_CLOSED_PATTERN,
                                    _first_index,
                                    _last_index.load());
    } else {
        return butil::string_printf(CURVE_SEGMENT_OPEN_PATTERN,
                                    _first_index);
    }
}

int CurveSegment::_load_entry(off_t offset, EntryHeader* head,
                              butil::IOBuf* data, size_t size_hint) const {
    butil::IOPortal buf;
    size_t to_read = std::max(size_hint, kEntryHeaderSize);
    const ssize_t n = braft::file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }

    char header_buf[kEntryHeaderSize];
    const char *p = (const char *)buf.fetch(header_buf, kEntryHeaderSize);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_real_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    butil::RawUnpacker un_packer(p);
    un_packer.unpack64((uint64_t&)term)
             .unpack32(meta_field)
             .unpack32(data_len)
             .unpack32(data_real_len)
             .unpack32(data_checksum)
             .unpack32(header_checksum);
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_real_len = data_real_len;
    tmp.data_checksum = data_checksum;
    if (!verify_checksum(tmp.checksum_type,
                        p, kEntryHeaderSize - 4, header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path;
        return -1;
    }
    if (head != NULL) {
        *head = tmp;
    }
    if (data != NULL) {
        if (buf.length() < kEntryHeaderSize + data_real_len) {
            const size_t to_read = kEntryHeaderSize + data_real_len
                                                    - buf.length();
            const ssize_t n = braft::file_pread(&buf, _fd,
                                    offset + buf.length(), to_read);
            if (n != (ssize_t)to_read) {
                return n < 0 ? -1 : 1;
            }
        } else if (buf.length() > kEntryHeaderSize + data_real_len) {
            buf.pop_back(buf.length() - kEntryHeaderSize - data_real_len);
        }
        CHECK_EQ(buf.length(), kEntryHeaderSize + data_real_len);
        buf.pop_front(kEntryHeaderSize);
        if (!verify_checksum(tmp.checksum_type, buf, tmp.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset="
                       << offset + kEntryHeaderSize
                       << " header=" << tmp
                       << " path: " << _path;
            return -1;
        }
        data->swap(buf);
    }
    return 0;
}

int CurveSegment::append(const braft::LogEntry* entry) {
    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        return EINVAL;
    } else if (entry->id.index !=
                    _last_index.load(butil::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        return ERANGE;
    }
    butil::IOBuf data;
    switch (entry->type) {
    case braft::ENTRY_TYPE_DATA:
        data.append(entry->data);
        break;
    case braft::ENTRY_TYPE_NO_OP:
        break;
    case braft::ENTRY_TYPE_CONFIGURATION:
        {
            butil::Status status = serialize_configuration_meta(entry, data);
            if (!status.ok()) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: "
                           << _path;
                return -1;
            }
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type
                   << ", path: " << _path;
        return -1;
    }
    uint32_t data_check_sum = get_checksum(_checksum_type, data);
    uint32_t real_length = data.length();
    size_t to_write = kEntryHeaderSize + data.length();
    uint32_t zero_bytes_num = 0;
    // 4KB alignment
    if (to_write % FLAGS_walAlignSize != 0) {
        zero_bytes_num = (to_write / FLAGS_walAlignSize + 1) *
                                        FLAGS_walAlignSize - to_write;
    }
    data.resize(data.length() + zero_bytes_num);
    to_write = kEntryHeaderSize + data.length();
    CHECK_LE(data.length(), 1ul << 56ul);
    char* write_buf = nullptr;
    if (FLAGS_enableWalDirectWrite) {
        int ret = posix_memalign(reinterpret_cast<void **>(&write_buf),
                                 FLAGS_walAlignSize, to_write);
        LOG_IF(FATAL, ret < 0 || write_buf == nullptr)
        << "posix_memalign WAL write buffer failed " << strerror(ret);
    } else {
        write_buf = new char[kEntryHeaderSize];
    }

    const uint32_t meta_field = (entry->type << 24) | (_checksum_type << 16);
    butil::RawPacker packer(write_buf);
    packer.pack64(entry->id.term)
          .pack32(meta_field)
          .pack32((uint32_t)data.length())
          .pack32(real_length)
          .pack32(data_check_sum);
    packer.pack32(get_checksum(
                  _checksum_type, write_buf, kEntryHeaderSize - 4));
    if (FLAGS_enableWalDirectWrite) {
        data.copy_to(write_buf + kEntryHeaderSize, real_length);
        int ret = ::pwrite(_direct_fd, write_buf, to_write, _meta.bytes);
        free(write_buf);
        if (ret != to_write) {
            LOG(ERROR) << "Fail to write directly to fd=" << _direct_fd
                       << ", buf=" << write_buf << ", size=" << to_write
                       << ", offset=" << _meta.bytes << ", error=" << berror();
            return -1;
        }
    } else {
        butil::IOBuf header;
        header.append(write_buf, kEntryHeaderSize);
        delete write_buf;
        butil::IOBuf* pieces[2] = { &header, &data };
        size_t start = 0;
        ssize_t written = 0;
        while (written < (ssize_t)to_write) {
            const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(
                    _fd, pieces + start, ARRAY_SIZE(pieces) - start);
            if (n < 0) {
                LOG(ERROR) << "Fail to write to fd=" << _fd
                           << ", path: " << _path << berror();
                return -1;
            }
            written += n;
            for (; start < ARRAY_SIZE(pieces) && pieces[start]->empty();
                    ++start) {}
        }
    }
    {
        BAIDU_SCOPED_LOCK(_mutex);
        _offset_and_term.push_back(std::make_pair(_meta.bytes, entry->id.term));
        _last_index.fetch_add(1, butil::memory_order_relaxed);
        _meta.bytes += to_write;
    }
    return _update_meta_page();
}

int CurveSegment::_update_meta_page() {
    char* metaPage = nullptr;
    int ret = posix_memalign(reinterpret_cast<void **>(&metaPage),
                            FLAGS_walAlignSize, _meta_page_size);
    LOG_IF(FATAL, ret < 0 || metaPage == nullptr)
        << "posix_memalign WAL meta page failed " << strerror(ret);
    memset(metaPage, 0, _meta_page_size);
    memcpy(metaPage, &_meta.bytes, sizeof(_meta.bytes));
    if (FLAGS_enableWalDirectWrite) {
        ret = ::pwrite(_direct_fd, metaPage, _meta_page_size, 0);
    } else {
        ret = ::pwrite(_fd, metaPage, _meta_page_size, 0);
    }
    free(metaPage);
    if (ret != _meta_page_size) {
        LOG(ERROR) << "Fail to write meta page into fd="
                   << (FLAGS_enableWalDirectWrite ? _direct_fd : _fd)
                   << ", path: " << _path << berror();
        return -1;
    }
    return 0;
}

braft::LogEntry* CurveSegment::get(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return NULL;
    }

    bool ok = true;
    braft::LogEntry* entry = NULL;
    do {
        braft::ConfigurationPBMeta configuration_meta;
        EntryHeader header;
        butil::IOBuf data;
        if (_load_entry(meta.offset, &header, &data,
                        meta.length) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new braft::LogEntry();
        entry->AddRef();
        switch (header.type) {
        case braft::ENTRY_TYPE_DATA:
            entry->data.swap(data);
            break;
        case braft::ENTRY_TYPE_NO_OP:
            CHECK(data.empty()) << "Data of NO_OP must be empty";
            break;
        case braft::ENTRY_TYPE_CONFIGURATION:
            {
                butil::Status status = parse_configuration_meta(data, entry);
                if (!status.ok()) {
                    LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                 << _path;
                    ok = false;
                    break;
                }
            }
            break;
        default:
            CHECK(false) << "Unknown entry type, path: " << _path;
            break;
        }

        if (!ok) {
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
        entry->type = (braft::EntryType)header.type;
    } while (0);

    if (!ok && entry != NULL) {
        entry->Release();
        entry = NULL;
    }
    return entry;
}

int CurveSegment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(butil::memory_order_relaxed)
                    || index < _first_index) {
        // out of range
        BRAFT_VLOG << "_last_index="
                   << _last_index.load(butil::memory_order_relaxed)
                   << " _first_index=" << _first_index;
        return -1;
    } else if (_last_index == _first_index - 1) {
        BRAFT_VLOG << "_last_index="
                   << _last_index.load(butil::memory_order_relaxed)
                   << " _first_index=" << _first_index;
        // empty
        return -1;
    }
    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor = (index <
                        _last_index.load(butil::memory_order_relaxed))
                      ? _offset_and_term[meta_index + 1].first : _meta.bytes;
    DCHECK_LT(entry_cursor, next_cursor);
    meta->offset = entry_cursor;
    meta->term = _offset_and_term[meta_index].second;
    meta->length = next_cursor - entry_cursor;
    return 0;
}

int64_t CurveSegment::get_term(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return 0;
    }
    return meta.term;
}

int CurveSegment::close(bool will_sync) {
    CHECK(_is_open);

    std::string old_path(_path);
    butil::string_appendf(&old_path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                         _first_index);
    std::string new_path(_path);
    butil::string_appendf(&new_path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                         _first_index, _last_index.load());

    LOG(INFO) << "close a full segment. Current first_index: " << _first_index
              << " last_index: " << _last_index
              << " raft_sync_segments: " << FLAGS_raftSyncSegments
              << " will_sync: " << will_sync
              << " path: " << new_path;
    int ret = 0;
    if (_last_index > _first_index) {
        if (FLAGS_raftSyncSegments && will_sync &&
                                !FLAGS_enableWalDirectWrite) {
            ret = braft::raft_fsync(_fd);
        }
    }

    _offset_and_term.shrink_to_fit();

    if (ret == 0) {
        _is_open = false;
        const int rc = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, rc == 0) << "Renamed `" << old_path
                              << "' to `" << new_path <<'\'';
        LOG_IF(ERROR, rc != 0) << "Fail to rename `" << old_path
                               << "' to `" << new_path <<"\', "
                               << berror();
        return rc;
    }
    return ret;
}

int CurveSegment::sync(bool will_sync) {
    if (_last_index > _first_index) {
        // CHECK(_is_open);
        if (!FLAGS_enableWalDirectWrite && braft::FLAGS_raft_sync
                                            && will_sync) {
            return braft::raft_fsync(_fd);
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

int CurveSegment::unlink() {
    int ret = 0;
    std::string path(_path);
    if (_is_open) {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                              _first_index);
    } else {
        butil::string_appendf(&path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());
    }
    int res = _walFilePool->RecycleFile(path);
    if (res != 0) {
        LOG(ERROR) << "Return segment to chunk file pool fail!";
        return -1;
    }
    LOG(INFO) << "Unlinked segment `" << path << '\'';
    return 0;
}

int CurveSegment::truncate(const int64_t last_index_kept) {
    int64_t truncate_size = 0;
    int64_t first_truncate_in_offset = 0;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (last_index_kept >= _last_index) {
        return 0;
    }
    first_truncate_in_offset = last_index_kept + 1 - _first_index;
    truncate_size = _offset_and_term[first_truncate_in_offset].first;
    BRAFT_VLOG << "Truncating " << _path << " first_index: " << _first_index
              << " last_index from " << _last_index << " to " << last_index_kept
              << " truncate size to " << truncate_size;
    lck.unlock();

    // Truncate on a full segment need to rename back to inprogess segment
    // again, because the node may crash before truncate.
    if (!_is_open) {
        std::string old_path(_path);
        butil::string_appendf(&old_path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                              _first_index, _last_index.load());
        std::string new_path(_path);
        butil::string_appendf(&new_path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                              _first_index);
        int ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                               << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
        if (ret != 0) {
            return ret;
        }
        _is_open = true;
    }

    _meta.bytes = truncate_size;
    int ret = _update_meta_page();
    if (ret < 0) {
        return ret;
    }

    // seek fd
    off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _fd << " to size="
                    << truncate_size << " path: " << _path;
        return -1;
    }

    lck.lock();
    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, butil::memory_order_relaxed);
    _meta.bytes = truncate_size;
    return 0;
}

}  // namespace chunkserver
}  // namespace curve
