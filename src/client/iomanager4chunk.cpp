/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 3:48:08 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include "include/curve_compiler_specific.h"
#include "src/client/client_config.h"
#include "src/client/iomanager4chunk.h"
#include "src/client/io_tracker.h"
#include "src/client/splitor.h"

namespace curve {
namespace client {
IOManager4Chunk::IOManager4Chunk() {
}

bool IOManager4Chunk::Initialize(IOOption_t ioOpt) {
    ioopt_ = ioOpt;
    mc_.Init(ioopt_.metaCacheOpt);
    Splitor::Init(ioopt_.ioSplitOpt);
    scheduler_ = new (std::nothrow) RequestScheduler();
    if (-1 == scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_)) {
        LOG(ERROR) << "Init scheduler_ failed!";
        return false;
    }

    scheduler_->Run();
    return true;
}

void IOManager4Chunk::UnInitialize() {
    scheduler_->Fini();
    delete scheduler_;
    scheduler_ = nullptr;
}

int IOManager4Chunk::ReadSnapChunk(LogicPoolID lpid,
                                        CopysetID cpid,
                                        ChunkID chunkid,
                                        uint64_t seq,
                                        uint64_t offset,
                                        uint64_t len,
                                        void *buf) {
    IOTracker temp(this, &mc_, scheduler_);
    temp.ReadSnapChunk(lpid, cpid, chunkid, seq, offset, len,
                       static_cast<char*>(buf));
    return temp.Wait();
}

int IOManager4Chunk::DeleteSnapChunk(LogicPoolID lpid,
                                            CopysetID cpid,
                                            ChunkID chunkid,
                                            uint64_t seq) {
    IOTracker temp(this, &mc_, scheduler_);
    temp.DeleteSnapChunk(lpid, cpid, chunkid, seq);
    return temp.Wait();
}

int IOManager4Chunk::GetChunkInfo(LogicPoolID lpid,
                                        CopysetID cpid,
                                        ChunkID chunkid,
                                        ChunkInfoDetail *chunkInfo) {
    IOTracker temp(this, &mc_, scheduler_);
    temp.GetChunkInfo(lpid, cpid, chunkid, chunkInfo);
    return temp.Wait();
}

void IOManager4Chunk::HandleAsyncIOResponse(IOTracker* iotracker) {
}

}   // namespace client
}   // namespace curve
