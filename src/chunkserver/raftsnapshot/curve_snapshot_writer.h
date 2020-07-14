/*
 * Project: curve
 * Created Date: 2020-06-11
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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_WRITER_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_WRITER_H_

#include <braft/storage.h>
#include <braft/snapshot.h>
#include <vector>
#include <string>
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"

namespace curve {
namespace chunkserver {

class CurveSnapshotWriter : public braft::SnapshotWriter {
 public:
    CurveSnapshotWriter(const std::string& path,
                        braft::FileSystemAdaptor* fs);
    virtual ~CurveSnapshotWriter();

    int64_t snapshot_index();
    virtual int init();
    virtual int save_meta(const braft::SnapshotMeta& meta);
    virtual std::string get_path() { return _path; }
    // Add file to the snapshot. It would fail it the file doesn't exist nor
    // references to any other file.
    // Returns 0 on success, -1 otherwise.
    virtual int add_file(const std::string& filename,
                         const ::google::protobuf::Message* file_meta);
    // Remove a file from the snapshot, it doesn't guarantees that the real file
    // would be removed from the storage.
    virtual int remove_file(const std::string& filename);
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename,
                              ::google::protobuf::Message* file_meta);
    // Sync meta table to disk
    int sync();
    braft::FileSystemAdaptor* file_system() { return _fs.get(); }

 private:
    std::string _path;
    braft::LocalSnapshotMetaTable _meta_table;
    scoped_refptr<braft::FileSystemAdaptor> _fs;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_WRITER_H_
