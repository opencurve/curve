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
 * File Created: Tuesday, 25th December 2018 3:18:12 pm
 * Author: tongguangxun
 */
#ifndef TEST_BACKUP_SNAPSHOT_INSTANCE_H_
#define TEST_BACKUP_SNAPSHOT_INSTANCE_H_
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <string>
#include <vector>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/chunk.pb.h"

#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/request_scheduler.h"
#include "src/client/service_helper.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {
class IOManager4Chunk;
class SnapInstance {
 public:
    SnapInstance();
    ~SnapInstance() = default;
    bool Initialize();
    void UnInitialize();

    int CreateSnapShot(std::string filename, uint64_t* seq);
    int DeleteSnapShot(std::string filename, uint64_t seq);
    int GetSnapShot(std::string filename, uint64_t seq, FInfo* snapif);
    int ListSnapShot(std::string filename,
                           const std::vector<uint64_t>* seq,
                           std::vector<FInfo*>* snapif);
    int GetSnapshotSegmentInfo(std::string filename,
                           uint64_t seq,
                           uint64_t offset,
                           SegmentInfo *segInfo);
    int ReadChunkSnapshot(LogicPoolID lpid,
                           CopysetID cpid,
                           ChunkID chunkID,
                           uint64_t seq,
                           uint64_t offset,
                           uint64_t len,
                           void *buf);
    int DeleteChunkSnapshotOrCorrectSn(LogicPoolID lpid,
                                      CopysetID cpid,
                                      ChunkID chunkID,
                                      uint64_t correctedSeq);
    int GetChunkInfo(LogicPoolID lpid,
                           CopysetID cpid,
                           ChunkID chunkID,
                           ChunkInfoDetail *chunkInfo);

 private:
    MDSClient               mdsclient_;
    MetaCache*              mc_;
    RequestScheduler*       scheduler_;
    RequestSenderManager*   reqsenderManager_;
    IOManager4Chunk*        ioctxManager_;
};
}   // namespace client
}   // namespace curve

#endif  // TEST_BACKUP_SNAPSHOT_INSTANCE_H_
