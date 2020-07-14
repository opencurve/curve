/*
 * Project: curve
 * Created Date: 2020-06-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */

// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#ifndef  SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_SERVICE_H_
#define  SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_SERVICE_H_

#include <butil/memory/singleton.h>
#include <braft/file_service.pb.h>
#include <braft/util.h>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_file_reader.h"
#include "src/chunkserver/raftsnapshot/define.h"

namespace curve {
namespace chunkserver {

DECLARE_string(raft_snapshot_dir);

class BAIDU_CACHELINE_ALIGNMENT CurveFileService : public braft::FileService {
 public:
    static CurveFileService& GetInstance() {
        static CurveFileService curveFileService;
        return curveFileService;
    }
    void get_file(::google::protobuf::RpcController* controller,
                  const ::braft::GetFileRequest* request,
                  ::braft::GetFileResponse* response,
                  ::google::protobuf::Closure* done);
    int add_reader(braft::FileReader* reader, int64_t* reader_id);
    int remove_reader(int64_t reader_id);
    void set_snapshot_attachment(SnapshotAttachment *snapshot_attachment);
    void clear_snapshot_attachment() {
        BAIDU_SCOPED_LOCK(_mutex);
        auto ret = _snapshot_attachment.release();
    }

 private:
    CurveFileService();
    ~CurveFileService() {}
    typedef std::map<int64_t, scoped_refptr<braft::FileReader> > Map;
    braft::raft_mutex_t _mutex;
    int64_t _next_id;
    Map _reader_map;
    scoped_refptr<SnapshotAttachment> _snapshot_attachment;
};

extern CurveFileService &kCurveFileService;

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_CURVE_FILE_SERVICE_H_
