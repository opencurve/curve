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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_STORAGE_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_STORAGE_H_

#include <braft/storage.h>
#include <braft/snapshot.h>
#include <map>
#include <string>
#include <set>
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_reader.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_writer.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_copier.h"

namespace curve {
namespace chunkserver {

// SnapshotStorage specific for curve, to fit the lightweight snapshot
// and snapshot consistency
class CurveSnapshotStorage : public braft::SnapshotStorage {
friend class CurveSnapshotCopier;
 public:
    explicit CurveSnapshotStorage(const std::string& path);
    CurveSnapshotStorage() {}
    virtual ~CurveSnapshotStorage() {}

    static const char* _s_temp_path;

    int init() override;
    braft::SnapshotWriter* create() override WARN_UNUSED_RESULT;
    int close(braft::SnapshotWriter* writer);

    braft::SnapshotReader* open() override WARN_UNUSED_RESULT;
    int close(braft::SnapshotReader* reader);
    braft::SnapshotReader* copy_from(
                const std::string& uri) override WARN_UNUSED_RESULT;
    braft::SnapshotCopier* start_to_copy_from(const std::string& uri) override;
    int close(braft::SnapshotCopier* copier) override;
    int set_filter_before_copy_remote() override;
    int set_file_system_adaptor(braft::FileSystemAdaptor* fs) override;
    int set_snapshot_throttle(
                    braft::SnapshotThrottle* snapshot_throttle) override;

    braft::SnapshotStorage* new_instance(const std::string& uri) const override;
    static void set_server_addr(const butil::EndPoint& server_addr) {
        _addr = server_addr;
    }
    static bool has_server_addr() { return _addr != butil::EndPoint(); }

 private:
    braft::SnapshotWriter* create(bool from_empty) WARN_UNUSED_RESULT;
    int destroy_snapshot(const std::string& path);
    int close(braft::SnapshotWriter* writer, bool keep_data_on_error);
    void ref(const int64_t index);
    void unref(const int64_t index);

    braft::raft_mutex_t _mutex;
    std::string _path;
    bool _filter_before_copy_remote;
    int64_t _last_snapshot_index;
    std::map<int64_t, int> _ref_map;
    scoped_refptr<braft::FileSystemAdaptor> _fs;
    scoped_refptr<braft::SnapshotThrottle> _snapshot_throttle;
    static butil::EndPoint _addr;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_STORAGE_H_
