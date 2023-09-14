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
 * File Created: Monday, 17th September 2018 3:22:06 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_IO_TRACKER_H_
#define SRC_CLIENT_IO_TRACKER_H_

#include <butil/iobuf.h>

#include <atomic>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "proto/chunk.pb.h"
#include "src/client/client_common.h"
#include "src/client/io_condition_varaiable.h"
#include "src/client/mds_client.h"
#include "src/client/metacache.h"
#include "src/client/request_context.h"
#include "src/client/request_scheduler.h"
#include "src/common/throttle.h"

namespace curve {
namespace client {

using curve::common::Throttle;

class IOManager;
class FileSegment;
class DiscardTaskManager;

// IOTracker is used to track a user's IO, as a user's IO may cross chunkservers,
// Therefore, when it is actually distributed, it will be split into multiple small IOs and sent down concurrently. Therefore, we need to
// Track the execution status of the sent request.
class CURVE_CACHELINE_ALIGNMENT IOTracker {
    friend class Splitor;

 public:
    IOTracker(IOManager* iomanager,
              MetaCache* mc,
              RequestScheduler* scheduler,
              FileMetric* clientMetric = nullptr,
              bool disableStripe = false);

    ~IOTracker() = default;

    /**
     * @brief StartRead Sync Read
     * @param buf read buffer
     * @param offset read offset
     * @param length Read length
     * @param mdsclient transparently transmits to the splitter for communication with mds
     * @param fileInfo Basic information of the file corresponding to the current io
     */
    void StartRead(void* buf, off_t offset, size_t length, MDSClient* mdsclient,
                   const FInfo_t* fileInfo, Throttle* throttle = nullptr);

    /**
     * @brief StartWrite Sync Write
     * @param buf write buffer
     * @param offset write offset
     * @param length Write length
     * @param mdsclient transparently transmits to the splitter for communication with mds
     * @param fileInfo Basic information of the file corresponding to the current io
     */
    void StartWrite(const void* buf, off_t offset, size_t length,
                    MDSClient* mdsclient, const FInfo_t* fileInfo,
                    const FileEpoch* fEpoch,
                    Throttle* throttle = nullptr);

    /**
     * @brief start an async read operation
     * @param ctx async read context
     * @param mdsclient used to communicate with MDS
     * @param fileInfo current file info
     */
    void StartAioRead(CurveAioContext* ctx, MDSClient* mdsclient,
                      const FInfo_t* fileInfo, Throttle* throttle = nullptr);

    /**
     * @brief start an async write operation
     * @param ctx async write context
     * @param mdsclient used to communicate with MDS
     * @param fileInfo current file info
     * @param fEpoch  file epoch info
     */
    void StartAioWrite(CurveAioContext* ctx, MDSClient* mdsclient,
                       const FInfo_t* fileInfo,
                       const FileEpoch* fEpoch, Throttle* throttle = nullptr);

    void StartDiscard(off_t offset, size_t length, MDSClient* mdsclient,
                      const FInfo_t* fileInfo, DiscardTaskManager* taskManager);

    void StartAioDiscard(CurveAioContext* ctx, MDSClient* mdsclient,
                         const FInfo_t* fileInfo,
                         DiscardTaskManager* taskManager);

    /**
     * The chunk-related interfaces are intended for use by snapshots. The upper-level snapshot
     * and file interfaces are separate. However, in the IOTracker, they are unified so that the
     * lower levels do not need to be aware of the upper-level interface category.
     * @param: chunkidinfo The target chunk
     * @param: seq is the snapshot version number
     * @param: offset is the offset within the snapshot
     * @param: len is the length to be read
     * @param: buf is the read buffer
     * @param: scc is the asynchronous callback
     */
    void ReadSnapChunk(const ChunkIDInfo &cinfo,
                     uint64_t seq,
                     uint64_t offset,
                     uint64_t len,
                     char *buf,
                     SnapCloneClosure* scc);
    /**
     * Delete snapshots generated during this dump or left over from history
     * If no snapshot is generated during the dump process, modify the correctedSn of the chunk
     * @param: chunkidinfo is the target chunk
     * @param: seq is the version number that needs to be corrected
     */
    void DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &cinfo,
                     uint64_t correctedSeq);
    /**
     * Obtain the version information of the chunk, where chunkInfo is the output parameter
     * @param: chunkidinfo target chunk
     * @param: chunkInfo is the detailed information of the snapshot
     */
    void GetChunkInfo(const ChunkIDInfo &cinfo,
                     ChunkInfoDetail *chunkInfo);

    /**
     * @brief lazy Create clone chunk
     * @param: location is the URL of the data source
     * @param: chunkidinfo target chunk
     * @param: sn chunk's serial number
     * @param: correntSn used to modify the chunk when CreateCloneChunk
     * @param: chunkSize chunk size
     * @param: scc is an asynchronous callback
     */
    void CreateCloneChunk(const std::string& location,
                          const ChunkIDInfo& chunkidinfo, uint64_t sn,
                          uint64_t correntSn, uint64_t chunkSize,
                          SnapCloneClosure* scc);

    /**
     * @brief Actual recovery chunk data
     * @param: chunkidinfo chunkidinfo
     * @param: offset offset
     * @param: len length
     * @param: chunkSize Chunk size
     * @param: scc is an asynchronous callback
     */
    void RecoverChunk(const ChunkIDInfo& chunkIdInfo, uint64_t offset,
                      uint64_t len, SnapCloneClosure* scc);

    /**
     * Wait is used for synchronous interface waiting. When the user's IO is taken over
     * by client internal threads, the call can return to the upper layer. However, the user's synchronous IO semantics require waiting for the result to return before
     * returning to the upper layer, so Wait here will make the user thread wait.
     * @return: Returns read/write information. For asynchronous IO, it returns 0 or -1. 0 means success, -1 means failure. 
     *          For synchronous IO, it returns the length or -1. 'length' represents the actual read/write length, and -1 represents read/write failure.
     */
    int Wait();

    /**
     * Each request must have its own OP type, and an interface is provided here to obtain the type during IO splitting
     */
    OpType Optype() {return type_;}

    // Set operation type, test usage
    void SetOpType(OpType type) { type_ = type; }

    /**
     * Because client IOs are all sent asynchronously, and a single IO is split into multiple Requests,
     * after asynchronous IO returns, it should inform the IOTracker that the current request has returned.
     * This way, the tracker can handle the returned request.
     * @param: The asynchronous request to be processed.
     */
    void HandleResponse(RequestContext* reqctx);

    /**
     * Obtain the current tracker ID information
     */
    uint64_t GetID() const {
        return id_;
    }

    // set user data type
    void SetUserDataType(const UserDataType dataType) {
        userDataType_ = dataType;
    }

    /**
     * @brief prepare space to store read data
     * @param subIoCount #space to store read data
     */
    void PrepareReadIOBuffers(const uint32_t subIoCount) {
        readDatas_.resize(subIoCount);
    }

    void SetReadData(const uint32_t subIoIndex, const butil::IOBuf& data) {
        readDatas_[subIoIndex] = data;
    }

    bool IsStripeDisabled() const {
        return disableStripe_;
    }

    static void InitDiscardOption(const DiscardOption& opt);

 private:
    void ReleaseAllSegmentLocks();

    /**
     * When IO returns, call done, which is responsible for returning upwards
     */
    void Done();

    /**
     * When IO splitting or IO distribution fails, it needs to be called, set the return status, and return upwards
     */
    void ReturnOnFail();
    /**
     * The user's incoming large IO will be split into multiple sub IOs, and the sub IO resources will be reclaimed before returning here
     */
    void DestoryRequestList();

    /**
     * Fill in the request context common field
     * @param: IDInfo is the ID information of the chunk
     * @param: req is the request context to be filled in
     */
    void FillCommonFields(ChunkIDInfo idinfo, RequestContext* req);

    /**
     * Convert chunkserver errcode to libcurve client errode
     * @param: errcode is the error code on the chunkserver side
     * @param[out]: errout is libcurve's own errode
     */
    void ChunkServerErr2LibcurveErr(curve::chunkserver::CHUNK_OP_STATUS errcode,
                                    LIBCURVE_ERROR* errout);

    /**
     * Obtain an initialized RequestContext
     * @return: If allocation or initialization fails, return nullptr
     *          On the contrary, return a pointer
     */
    RequestContext* GetInitedRequestContext() const;

    // perform read operation
    void DoRead(MDSClient* mdsclient, const FInfo_t* fileInfo,
                Throttle* throttle);

    /**
     * @brief read from the source
     * @param reqCtxVec the read request context vector
     * @param userInfo the user info
     * @param mdsclient interact with metadata server
     * @return 0 success; -1 fail
     */
    int ReadFromSource(const std::vector<RequestContext*>& reqCtxVec,
                       const UserInfo_t& userInfo, MDSClient* mdsClient);

    // perform write operation
    void DoWrite(MDSClient* mdsclient, const FInfo_t* fileInfo,
                 const FileEpoch* fEpoch,
                 Throttle* throttle);

    void DoDiscard(MDSClient* mdsclient, const FInfo_t* fileInfo,
                   DiscardTaskManager* taskManager);

    int ToReturnCode() const {
        return errcode_ == LIBCURVE_ERROR::OK
                   ? (type_ != OpType::DISCARD ? length_ : 0)
                   : (-errcode_);
    }

 private:
    // IO type
    OpType  type_;

    // The current IO data content, where data is the buffer for reading and writing data
    off_t      offset_;
    uint64_t   length_;

    // user data pointer
    void* data_;

    // user data type
    UserDataType userDataType_;

    // save write data
    butil::IOBuf writeData_;

    // save read data
    std::vector<butil::IOBuf> readDatas_;

    // When a user sends synchronous IO, they need to wait in the upper layer because the client's
    // IO sending process is all asynchronous, so here we need to use a conditional variable to wait for asynchronous IO to return
    // Afterwards, the waiting condition variable is awakened and then returned upwards.
    IOConditionVariable  iocv_;

    // The context of asynchronous IO is called aioctx when asynchronous IO returns
    // Asynchronous callback for return.
    CurveAioContext* aioctx_;

    // The errorcode of the current IO
    LIBCURVE_ERROR errcode_;

    // The current IO is split into reqcount_ Small IO
    std::atomic<uint32_t> reqcount_;

    // The large IO is split into multiple requests, which are stored in the reqlist in China
    std::vector<RequestContext*>   reqlist_;

    // store segment indices that can be discarded
    std::unordered_set<SegmentIndex> discardSegments_;

    // metacache is the metadata information of the current fileinstance
    MetaCache* mc_;

    // The scheduler is used to separate user threads from the client's own threads
    // After the large IO is split, the split reqlist is passed to the scheduler and sent downwards
    RequestScheduler* scheduler_;

    // For asynchronous IO, the Tracker needs to notify the upper level that the current IO processing has ended
    // The iomanager can release the tracker
    IOManager* iomanager_;

    // Initiation time
    uint64_t opStartTimePoint_;

    // Metric statistics on the client side
    FileMetric* fileMetric_;

    // The ID of the current tracker
    uint64_t id_;

    // Asynchronous call callback pointer for snapshot cloning system
    SnapCloneClosure* scc_;

    bool disableStripe_;

    // read/write operations will hold segment's read lock,
    // so store corresponding segment lock and release after operations finished
    std::vector<FileSegment*> segmentLocks_;

    // ID generator
    static std::atomic<uint64_t> tracekerID_;

    static DiscardOption discardOption_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_IO_TRACKER_H_
