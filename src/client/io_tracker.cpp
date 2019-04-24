/*
 * Project: curve
 * File Created: Monday, 17th September 2018 3:26:18 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>

#include "src/client/splitor.h"
#include "src/client/iomanager.h"
#include "src/client/io_tracker.h"
#include "src/client/request_scheduler.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {

IOTracker::IOTracker(IOManager* iomanager,
                        MetaCache* mc,
                        RequestScheduler* scheduler):
                        mc_(mc),
                        iomanager_(iomanager),
                        scheduler_(scheduler) {
    aioctx_     = nullptr;
    data_       = nullptr;
    type_       = OpType::UNKNOWN;
    errcode_    = 0;
    offset_     = 0;
    length_     = 0;
    reqlist_.clear();
    reqcount_.store(0, std::memory_order_release);;
}

void IOTracker::StartRead(CurveAioContext* aioctx,
                            char* buf,
                            off_t offset,
                            size_t length,
                            MDSClient* mdsclient,
                            const FInfo_t* fi) {
    data_   = buf;
    offset_ = offset;
    length_ = length;
    aioctx_ = aioctx;
    type_   = OpType::READ;

    int ret = Splitor::IO2ChunkRequests(this,
                                        mc_,
                                        &reqlist_,
                                        data_,
                                        offset_,
                                        length_,
                                        mdsclient,
                                        fi);
    if (ret == 0) {
        reqcount_.store(reqlist_.size(), std::memory_order_release);
        ret = scheduler_->ScheduleRequest(reqlist_);
    }

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::StartWrite(CurveAioContext* aioctx,
                            const char* buf,
                            off_t offset,
                            size_t length,
                            MDSClient* mdsclient,
                            const FInfo_t* fi) {
    data_   = buf;
    offset_ = offset;
    length_ = length;
    aioctx_ = aioctx;
    type_   = OpType::WRITE;

    DVLOG(9) << "offset: " << aioctx->offset << " length: " << aioctx->length
             << " op: " << aioctx->op << " buf: " << *(unsigned int*)buf;

    int ret = Splitor::IO2ChunkRequests(this, mc_, &reqlist_, data_, offset_,
                                        length_, mdsclient, fi);
    if (ret == 0) {
        reqcount_.store(reqlist_.size(), std::memory_order_release);
        ret = scheduler_->ScheduleRequest(reqlist_);
    }

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::ReadSnapChunk(const ChunkIDInfo &cinfo,
                              uint64_t seq,
                              uint64_t offset,
                              uint64_t len,
                              char *buf) {
    data_   = buf;
    offset_ = offset;
    length_ = len;
    type_   = OpType::READ_SNAP;

    int ret = -1;
    do {
        ret = Splitor::SingleChunkIO2ChunkRequests(this, mc_, &reqlist_, cinfo,
                                            data_, offset_, length_, seq);
        if (ret == 0) {
            reqcount_.store(reqlist_.size(), std::memory_order_release);
            ret = scheduler_->ScheduleRequest(reqlist_);
        }
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &cinfo,
                                uint64_t correctedSeq) {
    type_ = OpType::DELETE_SNAP;

    int ret = -1;
    do {
        RequestContext* newreqNode = new (std::nothrow) RequestContext();
        if (newreqNode == nullptr || !newreqNode->Init()) {
            LOG(ERROR) << "allocate req node failed!";
            break;
        }

        newreqNode->correctedSeq_ = correctedSeq;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::GetChunkInfo(const ChunkIDInfo &cinfo,
                            ChunkInfoDetail *chunkInfo) {
    type_ = OpType::GET_CHUNK_INFO;

    int ret = -1;
    do {
        RequestContext* newreqNode = new (std::nothrow) RequestContext();
        if (newreqNode == nullptr || !newreqNode->Init()) {
            LOG(ERROR) << "allocate req node failed!";
            break;
        }

        newreqNode->chunkinfodetail_ = chunkInfo;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::CreateCloneChunk(const std::string &location,
                                const ChunkIDInfo &cinfo,
                                uint64_t sn,
                                uint64_t correntSn,
                                uint64_t chunkSize) {
    type_ = OpType::CREATE_CLONE;

    int ret = -1;
    do {
        RequestContext* newreqNode = new (std::nothrow) RequestContext();
        if (newreqNode == nullptr || !newreqNode->Init()) {
            LOG(ERROR) << "allocate req node failed!";
            break;
        }

        newreqNode->seq_         = sn;
        newreqNode->chunksize_   = chunkSize;
        newreqNode->location_    = location;
        newreqNode->correctedSeq_  = correntSn;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::RecoverChunk(const ChunkIDInfo &cinfo,
                                        uint64_t offset,
                                        uint64_t len) {
    type_ = OpType::RECOVER_CHUNK;

    int ret = -1;
    do {
        RequestContext* newreqNode = new (std::nothrow) RequestContext();
        if (newreqNode == nullptr || !newreqNode->Init()) {
            LOG(ERROR) << "allocate req node failed!";
            break;
        }

        newreqNode->rawlength_   = len;
        newreqNode->offset_      = offset;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recyle resource!";
        ReturnOnFail();
    }
}

void IOTracker::FillCommonFields(ChunkIDInfo idinfo, RequestContext* req) {
    req->optype_      = type_;
    req->idinfo_      = idinfo;
    req->done_->SetIOTracker(this);
}

void IOTracker::HandleResponse(RequestContext* reqctx) {
    int errorcode = reqctx->done_->GetErrorCode();
    if (errorcode != 0) {
        errcode_ = -1;
    }

    if (1 == reqcount_.fetch_sub(1, std::memory_order_acq_rel)) {
        Done();
    }
}

int IOTracker::Wait() {
    return iocv_.Wait();
}

void IOTracker::Done() {
    DestoryRequestList();
    if (aioctx_ == nullptr) {
        errcode_ == 0 ? iocv_.Complete(length_) : iocv_.Complete(-1);
    } else {
        aioctx_->err = errcode_ == 0 ? LIBCURVE_ERROR::OK
                                     : LIBCURVE_ERROR::UNKNOWN;
        aioctx_->ret = errcode_ == 0 ? length_ : -1;
        aioctx_->cb(aioctx_);
        iomanager_->HandleAsyncIOResponse(this);
    }
}

void IOTracker::DestoryRequestList() {
    for (auto iter : reqlist_) {
        iter->UnInit();
        delete iter;
    }
}

void IOTracker::ReturnOnFail() {
    errcode_ = -1;
    Done();
}

}   // namespace client
}   // namespace curve
