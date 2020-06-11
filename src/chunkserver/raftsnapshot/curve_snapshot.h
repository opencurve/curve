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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_H_

#include <braft/storage.h>
#include <braft/snapshot.h>
#include <vector>
#include <string>
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"

namespace curve {
namespace chunkserver {

// Describe the Snapshot on another machine
class CurveSnapshot : public braft::Snapshot {
friend class CurveSnapshotCopier;
 public:
    // Get the path of the Snapshot
    std::string get_path() override;
    // List all the existing files in the Snapshot currently
    void list_files(std::vector<std::string> *files) override;
    // List all the attach files for the snapshot currently
    void list_attach_files(std::vector<std::string> *files);
    // Get the implementation-defined file_meta
    int get_file_meta(const std::string& filename,
                        ::google::protobuf::Message* file_meta) override;

 private:
    braft::LocalSnapshotMetaTable _meta_table;
    CurveSnapshotAttachMetaTable _attach_meta_table;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_H_
