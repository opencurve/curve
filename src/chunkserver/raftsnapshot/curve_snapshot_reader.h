/*
 * Project: curve
 * Created Date: 2020-06-09
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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_READER_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_READER_H_

#include <braft/storage.h>
#include <vector>
#include <string>
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"

namespace curve {
namespace chunkserver {

class CurveSnapshotReader : public braft::SnapshotReader {
 public:
    CurveSnapshotReader(const std::string& path,
                       const butil::EndPoint& server_addr,
                       braft::FileSystemAdaptor* fs,
                       braft::SnapshotThrottle* snapshot_throttle) :
        _path(path),
        _addr(server_addr),
        _reader_id(0),
        _fs(fs),
        _snapshot_throttle(snapshot_throttle) {}
    virtual ~CurveSnapshotReader();
    int64_t snapshot_index();
    virtual int init();
    int load_meta(braft::SnapshotMeta* meta) override;
    // Get the path of the Snapshot
    std::string get_path() override { return _path; }
    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    std::string generate_uri_for_copy() override;
    // List all the existing files in the Snapshot currently
    void list_files(std::vector<std::string> *files) override;

    // Get the implementation-defined file_meta
    int get_file_meta(const std::string& filename,
                        ::google::protobuf::Message* file_meta) override;

 private:
    void destroy_reader_in_file_service();

    std::string _path;
    braft::LocalSnapshotMetaTable _meta_table;
    butil::EndPoint _addr;
    int64_t _reader_id;
    scoped_refptr<braft::FileSystemAdaptor> _fs;
    scoped_refptr<braft::SnapshotThrottle> _snapshot_throttle;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_READER_H_
