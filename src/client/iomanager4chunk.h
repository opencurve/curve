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
 * File Created: Wednesday, 26th December 2018 3:47:53 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_IOMANAGER4CHUNK_H_
#define SRC_CLIENT_IOMANAGER4CHUNK_H_

#include <atomic>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/iomanager.h"
#include "src/client/metacache.h"
#include "src/client/request_scheduler.h"

namespace curve {
namespace client {
class MetaCache;
class IOManager4Chunk : public IOManager {
 public:
    IOManager4Chunk();
    ~IOManager4Chunk() = default;
    bool Initialize(IOOption ioOpt, MDSClient* mdsclient);

    /**
     * Read snapshot data of seq version number
     * @param: chunkidinfo target chunk
     * @param: seq is the snapshot version number
     * @param: offset is the offset within the snapshot
     * @param: len is the length to be read
     * @param: buf is a read buffer
     * @param: scc is an asynchronous callback
     * @return: Successfully returned the true read length, failed with -1
     */
    int ReadSnapChunk(const ChunkIDInfo& chunkidinfo, uint64_t seq,
                      uint64_t offset, uint64_t len, char* buf,
                      SnapCloneClosure* scc);
    /**
     * Delete snapshots generated during this dump or left over from history
     * If no snapshot is generated during the dump process, modify the
     * correctedSn of the chunk
     * @param: chunkidinfo target chunk
     * @param: correctedSeq is the version number that needs to be corrected
     */
    int DeleteSnapChunkOrCorrectSn(const ChunkIDInfo& chunkidinfo,
                                   uint64_t correctedSeq);
    /**
     * Obtain the version information of the chunk, where chunkInfo is the
     * output parameter
     * @param: chunkidinfo target chunk
     * @param: chunkInfo is the detailed information of the snapshot
     */
    int GetChunkInfo(const ChunkIDInfo& chunkidinfo,
                     ChunkInfoDetail* chunkInfo);

    /**
     * @brief lazy Create clone chunk
     * @detail
     *  - The format of the location is defined as A@B.
     *  - If the source data is on S3, the location format is uri@s3, where uri
     * is the actual address of the chunk object.
     *  - If the source data is on CurveFS, the location format is
     * /filename/chunkindex@cs.
     *
     * @param: location    URL of the data source
     * @param: chunkidinfo target chunk
     * @param: sn          chunk's serial number
     * @param: chunkSize   Chunk size
     * @param: correntSn   used to modify the chunk when creating CloneChunk
     * @param: scc is an asynchronous callback
     * @return successfully returns 0, otherwise -1
     */
    int CreateCloneChunk(const std::string& location,
                         const ChunkIDInfo& chunkidinfo, uint64_t sn,
                         uint64_t correntSn, uint64_t chunkSize,
                         SnapCloneClosure* scc);

    /**
     * @brief Actual recovery chunk data
     * @param chunkidinfo chunkidinfo
     * @param offset offset
     * @param len length
     * @param scc asynchronous callback
     * @return successfully returns 0, otherwise -1
     */
    int RecoverChunk(const ChunkIDInfo& chunkIdInfo, uint64_t offset,
                     uint64_t len, SnapCloneClosure* scc);

    /**
     * Because the bottom layer of the curve client is asynchronous IO, each IO
     * is assigned an IOtracker to track IO After this IO is completed, the
     * underlying layer needs to inform the current IO manager to release this
     * IOTracker, HandleAsyncIOResponse is responsible for releasing the
     * IOTracker
     * @param: It is an io returned asynchronously
     */
    void HandleAsyncIOResponse(IOTracker* iotracker) override;
    /**
     * Deconstruct and recycle resources
     */
    void UnInitialize();

    /**
     * Obtain Metacache, test code usage
     */
    MetaCache* GetMetaCache() { return &mc_; }
    /**
     * Set up scahuler to test code usage
     */
    void SetRequestScheduler(RequestScheduler* scheduler) {
        scheduler_ = scheduler;
    }

 private:
    // Each IOManager has its IO configuration, which is saved in the iooption
    IOOption ioopt_;

    // metacache stores the current snapshot client metadata information
    MetaCache mc_;

    // The IO is finally distributed by the schedule module to the chunkserver
    // end, and the scheduler is created and released by the IOManager
    RequestScheduler* scheduler_;
};

}  // namespace client
}  // namespace curve
#endif  // SRC_CLIENT_IOMANAGER4CHUNK_H_
