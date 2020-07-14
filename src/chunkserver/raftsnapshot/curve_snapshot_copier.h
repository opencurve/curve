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

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_COPIER_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_COPIER_H_

#include <braft/storage.h>
#include <vector>
#include <string>
#include "src/chunkserver/raftsnapshot/curve_snapshot.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_storage.h"

namespace curve {
namespace chunkserver {

class CurveSnapshotStorage;

class CurveSnapshotCopier : public braft::SnapshotCopier {
 public:
    CurveSnapshotCopier(CurveSnapshotStorage* storage,
                        bool filter_before_copy_remote,
                        braft::FileSystemAdaptor* fs,
                        braft::SnapshotThrottle* throttle);
    ~CurveSnapshotCopier();
    virtual void cancel();
    virtual void join();
    virtual braft::SnapshotReader* get_reader() { return _reader; }
    void start();
    int init(const std::string& uri);

 private:
    static void* start_copy(void* arg);
    void copy();
    void load_meta_table();
    void load_attach_meta_table();
    int filter_before_copy(CurveSnapshotWriter* writer,
                           braft::SnapshotReader* last_snapshot);
    void filter();
    void copy_file(const std::string& filename, bool attach = false);
    // 这里的filename是相对于快照目录的路径，为了先把文件下载到临时目录，需要把前面的..去掉
    std::string get_rfilename(const std::string& filename);

    braft::raft_mutex_t _mutex;
    bthread_t _tid;
    bool _cancelled;
    bool _filter_before_copy_remote;
    braft::FileSystemAdaptor* _fs;
    braft::SnapshotThrottle* _throttle;
    CurveSnapshotWriter* _writer;
    CurveSnapshotStorage* _storage;
    braft::SnapshotReader* _reader;
    braft::RemoteFileCopier::Session* _cur_session;
    CurveSnapshot _remote_snapshot;
    braft::RemoteFileCopier _copier;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_SNAPSHOT_COPIER_H_
