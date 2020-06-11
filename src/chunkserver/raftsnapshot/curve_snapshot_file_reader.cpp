/*
 * Project: curve
 * Created Date: 2020-06-10
 * Author: charisu
 * Copyright (c) 2018 netease
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
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"

namespace curve {
namespace chunkserver {

CurveSnapshotAttachMetaTable::CurveSnapshotAttachMetaTable() {}

CurveSnapshotAttachMetaTable::~CurveSnapshotAttachMetaTable() {}

int CurveSnapshotAttachMetaTable::add_attach_file(const std::string& filename,
                                                  const LocalFileMeta& meta) {
    Map::value_type value(filename, meta);
    std::pair<Map::iterator, bool> ret = _file_map.insert(value);
    LOG_IF(WARNING, !ret.second) << "attach file=" << filename
                                 << " already exists in snapshot";
    return ret.second ? 0 : -1;
}

int CurveSnapshotAttachMetaTable::load_from_iobuf_as_remote(
                                            const butil::IOBuf& buf) {
    CurveSnapshotPbAttachMeta pb_attach_meta;
    butil::IOBufAsZeroCopyInputStream wrapper(buf);
    if (!pb_attach_meta.ParseFromZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to parse LocalSnapshotPbMeta";
        return -1;
    }

    _file_map.clear();
    for (int i = 0; i < pb_attach_meta.files_size(); ++i) {
        const CurveSnapshotPbAttachMeta::File& f = pb_attach_meta.files(i);
        _file_map[f.name()] = f.meta();
    }
    return 0;
}

int CurveSnapshotAttachMetaTable::save_to_iobuf_as_remote(
                                            butil::IOBuf* buf) const {
    CurveSnapshotPbAttachMeta pb_attach_meta;
    for (Map::const_iterator
             iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        CurveSnapshotPbAttachMeta::File *f = pb_attach_meta.add_files();
        f->set_name(iter->first);
        *f->mutable_meta() = iter->second;
        f->mutable_meta()->clear_source();
    }
    buf->clear();
    butil::IOBufAsZeroCopyOutputStream wrapper(buf);
    return pb_attach_meta.SerializeToZeroCopyStream(&wrapper) ? 0 : -1;
}

void CurveSnapshotAttachMetaTable::list_files(
                                    std::vector<std::string>* files) const {
    if (!files) {
        return;
    }
    files->clear();
    files->reserve(_file_map.size());
    for (Map::const_iterator
             iter = _file_map.begin(); iter != _file_map.end(); ++iter) {
        files->push_back(iter->first);
    }
}

int CurveSnapshotAttachMetaTable::get_attach_file_meta(
                                        const std::string& filename,
                                        braft::LocalFileMeta* file_meta) const {
    Map::const_iterator iter = _file_map.find(filename);
    if (iter == _file_map.end()) {
        return -1;
    }
    if (file_meta) {
        LocalFileMeta meta = iter->second;
        file_meta->set_user_meta(meta.user_meta());
        file_meta->set_checksum(meta.checksum());
        if (meta.source() == FILE_SOURCE_LOCAL) {
            file_meta->set_source(braft::FILE_SOURCE_LOCAL);
        } else {
            file_meta->set_source(braft::FILE_SOURCE_REFERENCE);
        }
    }
    return 0;
}

int CurveSnapshotFileReader::read_file(butil::IOBuf* out,
                                      const std::string &filename,
                                      off_t offset,
                                      size_t max_count,
                                      bool read_partly,
                                      size_t* read_count,
                                      bool* is_eof) const {
    if (filename == BRAFT_SNAPSHOT_META_FILE) {
        int ret = _meta_table.save_to_iobuf_as_remote(out);
        if (ret == 0) {
            *read_count = out->size();
            *is_eof = true;
        }
        return ret;
    }
    braft::LocalFileMeta file_meta;
    if (_meta_table.get_file_meta(filename, &file_meta) != 0 &&
        _attach_meta_table.get_attach_file_meta(filename, nullptr)) {
        return EPERM;
    }
    size_t new_max_count = max_count;
    if (_snapshot_throttle &&
                braft::FLAGS_raft_enable_throttle_when_install_snapshot) {
        int ret = 0;
        int64_t start = butil::cpuwide_time_us();
        int64_t used_count = 0;
        new_max_count = _snapshot_throttle->throttled_by_throughput(max_count);
        if (new_max_count < max_count) {
            // if it's not allowed to read partly or it's allowed but
            // throughput is throttled to 0, try again.
            if (!read_partly || new_max_count == 0) {
                LOG(INFO) << "Read file throttled, path: " << path();
                ret = EAGAIN;
            }
        }
        if (ret == 0) {
            ret = LocalDirReader::read_file_with_meta(out, filename, &file_meta,
                                    offset, new_max_count, read_count, is_eof);
            used_count = out->size();
        }
        if ((ret == 0 || ret == EAGAIN) &&
                                used_count < (int64_t)new_max_count) {
            _snapshot_throttle->return_unused_throughput(
                new_max_count, used_count, butil::cpuwide_time_us() - start);
        }
        return ret;
    }
    return LocalDirReader::read_file_with_meta(out, filename, &file_meta,
                                    offset, new_max_count, read_count, is_eof);
}

}  // namespace chunkserver
}  // namespace curve
