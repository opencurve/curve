/*
 * Project: curve
 * Created Date: Saturday October 13th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/nameserver2/chunk_allocator.h"
#include "proto/nameserver2.pb.h"


namespace curve {
namespace mds {
bool ChunkSegmentAllocatorImpl::AllocateChunkSegment(FileType type,
        SegmentSizeType segmentSize, ChunkSizeType chunkSize,
        offset_t offset, PageFileSegment *segment)  {
        if (segment == nullptr) {
            LOG(ERROR) << "segment pointer is null";
            return false;
        }

        if (offset % segmentSize != 0) {
            LOG(ERROR) << "offset not align with segmentsize";
            return false;
        }
        if (chunkSize == 0 || chunkSize > segmentSize ||
                segmentSize % chunkSize != 0) {
            LOG(ERROR) << "chunkSize not align with segmentsize";
            return false;
        }

        segment->set_chunksize(chunkSize);
        segment->set_segmentsize(segmentSize);
        segment->set_startoffset(offset);

        // allocate chunks
        uint32_t chunkNum = segmentSize/chunkSize;
        std::vector<CopysetIdInfo> copysets;
        if (!topologyAdmin_->AllocateChunkRandomInSingleLogicalPool(
                type, chunkNum, &copysets)) {
            LOG(ERROR) << "AllocateChunkRandomInSingleLogicalPool error";
            return false;
        }
        if (copysets.size() != chunkNum) {
            LOG(ERROR) << "AllocateChunk return size error";
            return false;
        }
        auto logicalpoolId = copysets[0].logicalPoolId;
        for (auto i = 0; i != copysets.size(); i++) {
            if (copysets[i].logicalPoolId !=  logicalpoolId) {
                LOG(ERROR) << "Allocate Copysets id not same, copysets["
                            << i << "] = "
                            << copysets[i].logicalPoolId
                            << ", correct =" << logicalpoolId;
                return false;
            }
        }

        segment->set_logicalpoolid(logicalpoolId);

        for (uint32_t i = 0; i < chunkNum ; i++) {
            PageFileChunkInfo* chunkinfo =  segment->add_chunks();

            ChunkID chunkID;
            if (!chunkIDGenerator_->GenChunkID(&chunkID)) {
                LOG(ERROR) << "allocate error";
                return false;
            }
            chunkinfo->set_chunkid(chunkID);
            chunkinfo->set_copysetid(copysets[i].copySetId);
        }
        return true;
}

}   // namespace mds
}   // namespace curve

