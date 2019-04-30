/*
 * Project: curve
 * File Created: Tuesday, 25th December 2018 3:18:26 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <string>
#include <vector>
#include "test/backup/snapshot_instance.h"
#include "src/client/iomanager4chunk.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {
SnapInstance::SnapInstance() {
}

bool SnapInstance::Initialize() {
    bool inited = false;
    do {
        mc_ = new (std::nothrow) MetaCache();
        if (CURVE_UNLIKELY(mc_ == nullptr)) {
            LOG(ERROR) << "allocate metacache failed!";
            break;
        }

        if (mdsclient_.Initialize(ClientConfig::GetMetaServerOption().metaaddr) != 0) {  // NOLINT
            LOG(ERROR) << "MDSClient init failed!";
            break;
        }

        scheduler_ = new (std::nothrow) RequestScheduler();
        if (CURVE_UNLIKELY(scheduler_ == nullptr)) {
            LOG(ERROR) << "allocate RequestScheduler failed!";
            break;
        }

        reqsenderManager_ = new (std::nothrow) RequestSenderManager();
        if (CURVE_UNLIKELY(reqsenderManager_ == nullptr)) {
            LOG(ERROR) << "allocate RequestSenderManager failed!";
            break;
        }

        ioctxManager_ = new (std::nothrow) IOManager4Chunk(mc_, scheduler_);  //NOLINT
        if (CURVE_UNLIKELY(ioctxManager_ == nullptr)) {
            LOG(ERROR) << "allocate IOManager4File failed!";
            break;
        }

        if (!ioctxManager_->Initialize()) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }

        if (-1 == scheduler_->Init(ClientConfig::GetRequestSchedulerOption().queueCapacity, //NOLINT
                                    ClientConfig::GetRequestSchedulerOption().threadpoolSize, //NOLINT
                                    reqsenderManager_,
                                    mc_)) {
                LOG(ERROR) << "Init scheduler_ failed!";
                break;
        }

        scheduler_->Run();
        inited = true;
    } while (0);

    if (!inited) {
        delete ioctxManager_;
        delete scheduler_;
        delete reqsenderManager_;
        delete mc_;
        delete channel_;
    }
    return inited;
}

int SnapInstance::CreateSnapShot(std::string fname, uint64_t* seq) {
    return mdsclient_.CreateSnapShot(fname, seq);
}

int SnapInstance::DeleteSnapShot(std::string filename, uint64_t seq) {
    return mdsclient_.DeleteSnapShot(filename, seq);
}

int SnapInstance::GetSnapShot(std::string filename,
                                        uint64_t seq,
                                        FInfo* fi) {
    return mdsclient_.GetSnapShot(filename, seq, fi);
}

int SnapInstance::ListSnapShot(std::string filename,
                                         const std::vector<uint64_t>* seq,
                                         std::vector<FInfo*>* snapif) {
    return mdsclient_.ListSnapShot(filename, seq, snapif);
}

int SnapInstance::GetSnapshotSegmentInfo(std::string filename,
                                                    uint64_t seq,
                                                    uint64_t offset,
                                                    SegmentInfo *segif) {
    return mdsclient_.GetSnapshotSegmentInfo(filename, seq, offset, segif, mc_);
}

int SnapInstance::ReadChunkSnapshot(LogicPoolID lpid,
                                            CopysetID cpid,
                                            ChunkID chunkid,
                                            uint64_t seq,
                                            uint64_t offset,
                                            uint64_t len,
                                            void *buf) {
    return ioctxManager_->ReadSnapChunk(lpid, cpid, chunkid, seq, offset, len, buf);    // NOLINT
}

int SnapInstance::DeleteChunkSnapshotOrCorrectSn(LogicPoolID lpid,
                                                 CopysetID cpid,
                                                 ChunkID chunkid,
                                                 uint64_t correctedSeq) {
    return ioctxManager_->DeleteSnapChunkOrCorrectSn(
        lpid, cpid, chunkid, correctedSeq);
}

int SnapInstance::GetChunkInfo(LogicPoolID lpid,
                                            CopysetID cpid,
                                            ChunkID chunkid,
                                            ChunkInfoDetail *chunkInfo) {
    return ioctxManager_->GetChunkInfo(lpid, cpid, chunkid, chunkInfo);
}

void SnapInstance::UnInitialize() {
    scheduler_->Fini();
    ioctxManager_->UnInitialize();
    mdsclient_.UnInitialize();

    delete ioctxManager_;
    delete scheduler_;
    delete reqsenderManager_;

    scheduler_        = nullptr;
    ioctxManager_     = nullptr;
    reqsenderManager_ = nullptr;
}

}   // namespace client
}   // namespace curve
