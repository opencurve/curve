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
 * Created Date: 2020-06-10
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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_FILE_READER_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_FILE_READER_H_

#include <braft/file_reader.h>
#include <braft/snapshot.h>
#include <utility>
#include <vector>
#include <string>
#include <map>
#include "proto/curve_storage.pb.h"
#include "src/chunkserver/raftsnapshot/define.h"

namespace curve {
namespace chunkserver {

/**
 * snapshot attachment file metadata table, together with the above CurveSnapshotAttachMetaTable
 * interface, mainly provides the attach file metadata information query, serialization and deserialization interfaces
 */
class CurveSnapshotAttachMetaTable {
 public:
    CurveSnapshotAttachMetaTable();
    ~CurveSnapshotAttachMetaTable();
    // Add attach file to the meta
    int add_attach_file(const std::string& filename,
                        const LocalFileMeta& file_meta);
    // get attach file meta
    int get_attach_file_meta(const std::string& filename,
                             braft::LocalFileMeta* file_meta) const;
    // list files in the attach meta table
    void list_files(std::vector<std::string> *files) const;
    // deserialize
    int load_from_iobuf_as_remote(const butil::IOBuf& buf);
    // serialize
    int save_to_iobuf_as_remote(butil::IOBuf* buf) const;

 private:
    typedef std::map<std::string, LocalFileMeta> Map;
    // file -> file meta
    Map    _file_map;
};

class CurveSnapshotFileReader : public braft::LocalDirReader {
 public:
    CurveSnapshotFileReader(braft::FileSystemAdaptor* fs,
                           const std::string& path,
                           braft::SnapshotThrottle* snapshot_throttle)
            : LocalDirReader(fs, path),
              _snapshot_throttle(snapshot_throttle)
    {}
    virtual ~CurveSnapshotFileReader() = default;

    void set_meta_table(const braft::LocalSnapshotMetaTable &meta_table) {
        _meta_table = meta_table;
    }

    void set_attach_meta_table(
            const CurveSnapshotAttachMetaTable &attach_meta_table) {
        _attach_meta_table = attach_meta_table;
    }

    int read_file(butil::IOBuf* out,
                  const std::string &filename,
                  off_t offset,
                  size_t max_count,
                  bool read_partly,
                  size_t* read_count,
                  bool* is_eof) const override;

    braft::LocalSnapshotMetaTable get_meta_table() {
        return _meta_table;
    }

 private:
    braft::LocalSnapshotMetaTable _meta_table;
    CurveSnapshotAttachMetaTable _attach_meta_table;
    scoped_refptr<braft::SnapshotThrottle> _snapshot_throttle;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_FILE_READER_H_
