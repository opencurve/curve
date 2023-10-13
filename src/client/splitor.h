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
 * File Created: Monday, 17th September 2018 4:20:52 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_SPLITOR_H_
#define SRC_CLIENT_SPLITOR_H_

#include <butil/iobuf.h>

#include <string>
#include <vector>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/config_info.h"
#include "src/client/io_tracker.h"
#include "src/client/metacache.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

class MDSClient;
class IOTracker;
class FileSegment;

class Splitor {
 public:
    static void Init(const IOSplitOption& ioSplitOpt);

    /**
     * Split user IO into Chunk level IO
     * @param: iotracker Big IO Context Information
     * @param: metaCache is the cache information that needs to be used during
     * the IO splitting process
     * @param: targetlist The storage list of small IO after the large IO is
     * split
     * @param: data is the data to be written
     * @param: offset The actual offset of IO issued by the user
     * @param: length Data length
     * @param: mdsclient searches for information through mdsclient when
     * searching for metaahe fails
     * @param: fi stores some basic information about the current IO, such as
     * chunksize, etc
     * @param: FileEpoch_t file epoch information
     */
    static int IO2ChunkRequests(IOTracker* iotracker, MetaCache* metaCache,
                                std::vector<RequestContext*>* targetlist,
                                butil::IOBuf* data, off_t offset, size_t length,
                                MDSClient* mdsclient, const FInfo_t* fi,
                                const FileEpoch_t* fEpoch);

    /**
     * Fine grained splitting of single ChunkIO
     * @param: iotracker Big IO Context Information
     * @param: metaCache is the cache information that needs to be used during
     * the IO splitting process
     * @param: targetlist The storage list of small IO after the large IO is
     * split
     * @param: cid is the ID information of the current chunk
     * @param: data is the data to be written
     * @param: offset is the offset within the current chunk
     * @param: length Data length
     * @param: seq is the version number of the current chunk
     */
    static int SingleChunkIO2ChunkRequests(
        IOTracker* iotracker, MetaCache* metaCache,
        std::vector<RequestContext*>* targetlist, const ChunkIDInfo& cid,
        butil::IOBuf* data, off_t offset, size_t length, uint64_t seq);

    /**
     * @brief calculates the location information of the request
     * @param ioTracker io Context Information
     * @param metaCache file cache information
     * @param chunkIdx Current chunk information
     * @return source information
     */
    static RequestSourceInfo CalcRequestSourceInfo(IOTracker* ioTracker,
                                                   MetaCache* metaCache,
                                                   ChunkIndex chunkIdx);

    static bool NeedGetOrAllocateSegment(MetaCacheErrorType error,
                                         OpType opType,
                                         const ChunkIDInfo& chunkInfo,
                                         const MetaCache* metaCache);

 private:
    /**
     * IO2ChunkRequests will internally call this function for actual splitting
     * operations
     * @param: iotracker Big IO Context Information
     * @param: mc is the cache information that needs to be used during IO
     * splitting process
     * @param: targetlist The storage list of small IO after the large IO is
     * split
     * @param: Data is the data to be written
     * @param: offset The actual offset of IO issued by the user
     * @param: length Data length
     * @param: mdsclient searches for information through mdsclient when
     * searching for metaahe fails
     * @param: fi stores some basic information about the current IO, such as
     * chunksize, etc
     * @param: chunkidx is the index value of the current chunk in the vdisk
     */
    static bool AssignInternal(IOTracker* iotracker, MetaCache* metaCache,
                               std::vector<RequestContext*>* targetlist,
                               butil::IOBuf* data, off_t offset,
                               uint64_t length, MDSClient* mdsclient,
                               const FInfo_t* fi, const FileEpoch_t* fEpoch,
                               ChunkIndex chunkidx);

    static bool GetOrAllocateSegment(bool allocateIfNotExist, uint64_t offset,
                                     MDSClient* mdsClient, MetaCache* metaCache,
                                     const FInfo* fileInfo,
                                     const FileEpoch_t* fEpoch,
                                     ChunkIndex chunkidx);

    static int SplitForNormal(IOTracker* iotracker, MetaCache* metaCache,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fileInfo,
                              const FileEpoch_t* fEpoch);

    static int SplitForStripe(IOTracker* iotracker, MetaCache* metaCache,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fileInfo,
                              const FileEpoch_t* fEpoch);

    static bool MarkDiscardBitmap(IOTracker* iotracker,
                                  FileSegment* fileSegment,
                                  SegmentIndex segmentIndex, uint64_t offset,
                                  uint64_t len);

 private:
    // Configuration information used for IO split modules
    static IOSplitOption iosplitopt_;
};
}  // namespace client
}  // namespace curve
#endif  // SRC_CLIENT_SPLITOR_H_
