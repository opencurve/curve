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

bool IOManager4Chunk::Initialize(IOOption_t ioOpt, MDSClient* mdsclient) {
    ioopt_ = ioOpt;
    mc_.Init(ioopt_.metaCacheOpt, mdsclient);
    Splitor::Init(ioopt_.ioSplitOpt);
    scheduler_ = new (std::nothrow) RequestScheduler();
    if (scheduler_ == nullptr ||
        -1 == scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_, nullptr)) {
        LOG(ERROR) << "Init scheduler_ failed!";
        delete scheduler_;
        scheduler_ = nullptr;
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

int IOManager4Chunk::ReadSnapChunk(const ChunkIDInfo &chunkidinfo,
    uint64_t seq, uint64_t offset, uint64_t len,
    char *buf, SnapCloneClosure* scc) {

    IOTracker* temp = new IOTracker(this, &mc_, scheduler_);
    temp->ReadSnapChunk(chunkidinfo, seq, offset, len, buf, scc);
    return 0;
}

int IOManager4Chunk::DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &chunkidinfo,
    uint64_t correctedSeq) {

    IOTracker temp(this, &mc_, scheduler_);
    temp.DeleteSnapChunkOrCorrectSn(chunkidinfo, correctedSeq);
    return temp.Wait();
}

int IOManager4Chunk::GetChunkInfo(const ChunkIDInfo &chunkidinfo,
                                  ChunkInfoDetail *chunkInfo) {
    IOTracker temp(this, &mc_, scheduler_);
    temp.GetChunkInfo(chunkidinfo, chunkInfo);
    return temp.Wait();
}

int IOManager4Chunk::CreateCloneChunk(const std::string &location,
    const ChunkIDInfo &chunkidinfo, uint64_t sn, uint64_t correntSn,
    uint64_t chunkSize, SnapCloneClosure* scc) {

    IOTracker* temp = new IOTracker(this, &mc_, scheduler_);
    temp->CreateCloneChunk(location, chunkidinfo, sn,
                           correntSn, chunkSize, scc);
    return 0;
}

int IOManager4Chunk::RecoverChunk(const ChunkIDInfo &chunkidinfo,
    uint64_t offset, uint64_t len, SnapCloneClosure* scc) {

    IOTracker* temp = new IOTracker(this, &mc_, scheduler_);
    temp->RecoverChunk(chunkidinfo, offset, len, scc);
    return 0;
}

void IOManager4Chunk::HandleAsyncIOResponse(IOTracker* iotracker) {
    delete iotracker;
}

}   // namespace client
}   // namespace curve
