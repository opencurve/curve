/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:20:58 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/client/splitor.h"
#include "src/client/request_closure.h"

DECLARE_uint32(chunk_size);

namespace curve {
namespace client {

Splitor::Splitor() {
}

Splitor::~Splitor() {
}

int Splitor::IO2ChunkRequests(IOContext* ioctx,
                            MetaCache* mc,
                            RequestContextSlab* reqslab,
                            std::list<RequestContext*>* targetlist,
                            const char* data,
                            off_t offset,
                            size_t length) {
    if (reqslab == nullptr ||
        targetlist == nullptr ||
        mc == nullptr ||
        ioctx == nullptr ||
        data == nullptr) {
            return -1;
    }
    DVLOG(9) << "I/O request, offset: " << offset << " len:" << length
             << " chunksize: " << FLAGS_chunk_size;
    /* the io comes here, already 4k aligned */
    uint64_t startchunkindex = offset / FLAGS_chunk_size;
    uint64_t endchunkindex = (offset + length - 1) / FLAGS_chunk_size;
    uint64_t startoffset = offset % FLAGS_chunk_size;

    auto assignFunc = [&](const char* buf,
                        uint64_t off,
                        uint64_t len,
                        ChunkID chunkidx)->bool {
        auto newreqNode = reqslab->Get();
        newreqNode->data_ = buf;
        newreqNode->offset_ = off;
        newreqNode->rawlength_ = len;
        newreqNode->optype_ = ioctx->type_;
        Chunkinfo_t chinfo;
        DVLOG(9) << "Split I/O request, offset: " << off
                 << " len:" << length << " chunkidx: " << chunkidx;
        int ret = mc->GetChunkInfo(chunkidx, &chinfo);
        if (ret == 0) {
            newreqNode->chunkid_ = chinfo.chunkid_;
            newreqNode->copysetid_ = chinfo.copysetid_;
            newreqNode->logicpoolid_ = chinfo.logicpoolid_;
            newreqNode->done_->SetIOContext(ioctx);
            targetlist->push_back(newreqNode);
            return true;
        } else {
            LOG(ERROR) << "io Split got invalid chunk info, chunk index = "
                        << chunkidx
                        << ", offset = "
                        << off
                        << ", size = "
                        << len
                        << ", GetChunkInfo ret = "
                        << ret;
            return false;
        }
    };

    if (startchunkindex == endchunkindex) {
        DVLOG(9) << "I/O signle request, offset: " << startoffset
                 << " len:" << length << "chunkidx: " << startchunkindex;
        if (!assignFunc(data, startoffset, length, startchunkindex)) {
            return -1;
        }
    } else {
        DVLOG(9) << "I/O bundle request, offset: " << startoffset
                 << " len:" << FLAGS_chunk_size - startoffset
                 << "chunkidx: " << startchunkindex;
        if (!assignFunc(data,
                        startoffset,
                        FLAGS_chunk_size - startoffset,
                        startchunkindex)) {
            return -1;
        }
        uint64_t chunkindex = startchunkindex + 1;
        uint64_t tempoff = FLAGS_chunk_size - startoffset;
        uint64_t currentpos = offset + tempoff - 1;
        uint64_t endpos = offset + length;

        while (chunkindex <= endchunkindex) {
            uint64_t le = 0;
            if ((endpos - currentpos) >= FLAGS_chunk_size) {
                le = FLAGS_chunk_size;
            } else {
                le = endpos - currentpos - 1;
            }

            DVLOG(9) << "I/O bundle request, offset: " << 0
                     << " len:" << le << "chunkidx: " << chunkindex;
            if (!assignFunc(data + tempoff, 0, le, chunkindex)) {
                return -1;
            }
            tempoff += le;
            currentpos += le;
            chunkindex++;
        }
    }
    return 0;
}
}   // namespace client
}   // namespace curve
