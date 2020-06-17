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
 * Created Date: 2020-06-09
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

#include <vector>
#include <string>
#include "src/chunkserver/raftsnapshot/curve_snapshot_reader.h"
#include "src/chunkserver/raftsnapshot/curve_file_service.h"

namespace curve {
namespace chunkserver {

CurveSnapshotReader::~CurveSnapshotReader() {
    destroy_reader_in_file_service();
}

int CurveSnapshotReader::init() {
    if (!_fs->directory_exists(_path)) {
        set_error(ENOENT, "Not such _path : %s", _path.c_str());
        return ENOENT;
    }
    std::string meta_path = _path + "/" BRAFT_SNAPSHOT_META_FILE;
    if (_meta_table.load_from_file(_fs, meta_path) != 0) {
        set_error(EIO, "Fail to load meta");
        return EIO;
    }
    return 0;
}

int CurveSnapshotReader::load_meta(braft::SnapshotMeta* meta) {
    if (!_meta_table.has_meta()) {
        return -1;
    }
    *meta = _meta_table.meta();
    return 0;
}

int64_t CurveSnapshotReader::snapshot_index() {
    butil::FilePath path(_path);
    int64_t index = 0;
    int ret = sscanf(path.BaseName().value().c_str(),
                     BRAFT_SNAPSHOT_PATTERN, &index);
    CHECK_EQ(ret, 1);
    return index;
}

void CurveSnapshotReader::list_files(std::vector<std::string> *files) {
    return _meta_table.list_files(files);
}

int CurveSnapshotReader::get_file_meta(const std::string& filename,
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

std::string CurveSnapshotReader::generate_uri_for_copy() {
    if (_addr == butil::EndPoint()) {
        LOG(ERROR) << "Address is not specified, path:" << _path;
        return std::string();
    }

    if (_reader_id == 0) {
        scoped_refptr<CurveSnapshotFileReader> reader(
                new CurveSnapshotFileReader(_fs.get(), _path,
                                            _snapshot_throttle.get()));
        reader->set_meta_table(_meta_table);
        if (!reader->open()) {
            LOG(ERROR) << "Open snapshot=" << _path << " failed";
            return std::string();
        }
        if (kCurveFileService.add_reader(reader.get(), &_reader_id) != 0) {
            LOG(ERROR) << "Fail to add reader to file_service, path: " << _path;
            return std::string();
        }
    }
    std::ostringstream oss;
    oss << "remote://" << _addr << "/" << _reader_id;
    return oss.str();
}

void CurveSnapshotReader::destroy_reader_in_file_service() {
    if (_reader_id != 0) {
        CHECK_EQ(0, kCurveFileService.remove_reader(_reader_id));
        _reader_id = 0;
    }
}

}  // namespace chunkserver
}  // namespace curve
