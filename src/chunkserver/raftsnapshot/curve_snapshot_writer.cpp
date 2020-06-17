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
 * Created Date: 2020-06-11
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
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include "src/chunkserver/raftsnapshot/curve_snapshot_writer.h"

namespace curve {
namespace chunkserver {

CurveSnapshotWriter::CurveSnapshotWriter(const std::string& path,
                                         braft::FileSystemAdaptor* fs)
    : _path(path), _fs(fs) {
}

CurveSnapshotWriter::~CurveSnapshotWriter() {
}

int CurveSnapshotWriter::init() {
    butil::File::Error e;
    if (!_fs->create_directory(_path, &e, false)) {
        set_error(EIO, "CreateDirectory failed, path: %s", _path.c_str());
        return EIO;
    }
    std::string meta_path = _path + "/" BRAFT_SNAPSHOT_META_FILE;
    if (_fs->path_exists(meta_path) &&
                _meta_table.load_from_file(_fs, meta_path) != 0) {
        set_error(EIO, "Fail to load metatable from %s", meta_path.c_str());
        return EIO;
    }

    // remove file if meta_path not exist or it's not in _meta_table
    // to avoid dirty data
    {
         std::vector<std::string> to_remove;
         braft::DirReader* dir_reader = _fs->directory_reader(_path);
         if (!dir_reader->is_valid()) {
             LOG(WARNING) << "directory reader failed, maybe NOEXIST or"
                 " PERMISSION, path: " << _path;
             delete dir_reader;
             return EIO;
         }
         while (dir_reader->next()) {
             std::string filename = dir_reader->name();
             if (filename != BRAFT_SNAPSHOT_META_FILE) {
                 if (get_file_meta(filename, NULL) != 0) {
                     to_remove.push_back(filename);
                 }
             }
         }
         delete dir_reader;
         for (size_t i = 0; i < to_remove.size(); ++i) {
             std::string file_path = _path + "/" + to_remove[i];
             _fs->delete_file(file_path, false);
             LOG(WARNING) << "Snapshot file exist but meta not found so"
                 " delete it, path: " << file_path;
         }
    }

    return 0;
}

int64_t CurveSnapshotWriter::snapshot_index() {
    return _meta_table.has_meta() ?
                _meta_table.meta().last_included_index() : 0;
}

int CurveSnapshotWriter::remove_file(const std::string& filename) {
    return _meta_table.remove_file(filename);
}

int CurveSnapshotWriter::add_file(
        const std::string& filename,
        const ::google::protobuf::Message* file_meta) {
    braft::LocalFileMeta meta;
    if (file_meta) {
        meta.CopyFrom(*file_meta);
    }
    return _meta_table.add_file(filename, meta);
}

void CurveSnapshotWriter::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int CurveSnapshotWriter::get_file_meta(const std::string& filename,
                                       ::google::protobuf::Message* file_meta) {
    braft::LocalFileMeta* meta = NULL;
    if (file_meta) {
        meta = dynamic_cast<braft::LocalFileMeta*>(file_meta);
        if (meta == NULL) {
            return -1;
        }
    }
    return _meta_table.get_file_meta(filename, meta);
}

int CurveSnapshotWriter::save_meta(const braft::SnapshotMeta& meta) {
    _meta_table.set_meta(meta);
    return 0;
}

int CurveSnapshotWriter::sync() {
    const int rc = _meta_table.save_to_file(
                        _fs, _path + "/" BRAFT_SNAPSHOT_META_FILE);
    if (rc != 0 && ok()) {
        LOG(ERROR) << "Fail to sync, path: " << _path;
        set_error(rc, "Fail to sync : %s", berror(rc));
    }
    return rc;
}

}  // namespace chunkserver
}  // namespace curve
