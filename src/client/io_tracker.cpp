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
 * File Created: Monday, 17th September 2018 3:26:18 pm
 * Author: tongguangxun
 */

#include <glog/logging.h>

#include <algorithm>

#include "src/client/splitor.h"
#include "src/client/iomanager.h"
#include "src/client/io_tracker.h"
#include "src/client/request_scheduler.h"
#include "src/client/request_closure.h"
#include "src/common/timeutility.h"
#include "src/client/libcurve_file.h"
#include "src/client/source_reader.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;

std::atomic<uint64_t> IOTracker::tracekerID_(1);

IOTracker::IOTracker(IOManager* iomanager,
                     MetaCache* mc,
                     RequestScheduler* scheduler,
                     FileMetric* clientMetric,
                     bool disableStripe)
    : mc_(mc),
      iomanager_(iomanager),
      scheduler_(scheduler),
      fileMetric_(clientMetric),
      disableStripe_(disableStripe) {
    id_         = tracekerID_.fetch_add(1, std::memory_order_relaxed);
    scc_        = nullptr;
    aioctx_     = nullptr;
    data_       = nullptr;
    type_       = OpType::UNKNOWN;
    errcode_    = LIBCURVE_ERROR::OK;
    offset_     = 0;
    length_     = 0;
    reqlist_.clear();
    reqcount_.store(0, std::memory_order_release);
    opStartTimePoint_ = curve::common::TimeUtility::GetTimeofDayUs();
}

void IOTracker::StartRead(void* buf, off_t offset, size_t length,
                          MDSClient* mdsclient, const FInfo_t* fileInfo) {
    data_ = buf;
    offset_ = offset;
    length_ = length;
    type_ = OpType::READ;

    VLOG(3) << "read op, offset = " << offset << ", length = " << length;

    DoRead(mdsclient, fileInfo);
}

void IOTracker::StartAioRead(CurveAioContext* ctx, MDSClient* mdsclient,
                             const FInfo_t* fileInfo) {
    aioctx_ = ctx;
    data_ = ctx->buf;
    offset_ = ctx->offset;
    length_ = ctx->length;
    type_ = OpType::READ;

    VLOG(3) << "aioread op, offset = " << ctx->offset
            << ", length = " << ctx->length;

    DoRead(mdsclient, fileInfo);
}

void IOTracker::StartAioReadSnapshot(CurveAioContext* ctx, MDSClient* mdsclient,
                             const FInfo_t* fileInfo) {
    aioctx_ = ctx;
    data_ = ctx->buf;
    offset_ = ctx->offset;
    length_ = ctx->length;
    type_ = OpType::READ_SNAP;

    DVLOG(9) << "aioreadsnapshot op, offset = " << ctx->offset
             << ", length = " << ctx->length;

    DoRead(mdsclient, fileInfo);
}

void IOTracker::DoRead(MDSClient* mdsclient, const FInfo_t* fileInfo) {
    int ret = Splitor::IO2ChunkRequests(this, mc_, &reqlist_, nullptr, offset_,
                                        length_, mdsclient, fileInfo, nullptr);
    if (ret == 0) {
        PrepareReadIOBuffers(reqlist_.size());
        uint32_t subIoIndex = 0;
        std::vector<RequestContext*> originReadVec;

        std::for_each(reqlist_.begin(), reqlist_.end(), [&](RequestContext* r) {
            // fake subrequest
            if (!r->idinfo_.chunkExist) {
                // the clone source is empty
                if (r->sourceInfo_.cloneFileSource.empty()) {
                    // add zero data
                    r->readData_.resize(r->rawlength_, 0);
                    r->done_->SetFailed(LIBCURVE_ERROR::OK);
                } else {
                    // read from original volume
                    originReadVec.emplace_back(r);
                }
            }

            r->done_->SetFileMetric(fileMetric_);
            r->done_->SetIOManager(iomanager_);
            r->subIoIndex_ = subIoIndex++;
        });

        reqcount_.store(reqlist_.size(), std::memory_order_release);
        if (scheduler_->ScheduleRequest(reqlist_) == 0 &&
            ReadFromSource(originReadVec, fileInfo->userinfo, mdsclient) == 0) {
            ret = 0;
        } else {
            ret = -1;
        }
    } else {
        LOG(ERROR) << "splitor read io failed, "
                   << "offset = " << offset_ << ", length = " << length_;
    }

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recycle resource!";
        ReturnOnFail();
    }
}

int IOTracker::ReadFromSource(const std::vector<RequestContext*>& reqCtxVec,
                              const UserInfo_t& userInfo,
                              MDSClient* mdsClient) {
    if (reqCtxVec.empty()) {
        return 0;
    }

    return SourceReader::GetInstance().Read(reqCtxVec, userInfo, mdsClient);
}

void IOTracker::StartWrite(const void* buf, off_t offset, size_t length,
                           MDSClient* mdsclient, const FInfo_t* fileInfo,
                           const FileEpoch* fEpoch) {
    data_ = const_cast<void*>(buf);
    offset_ = offset;
    length_ = length;
    type_ = OpType::WRITE;

    VLOG(3) << "write op, offset = " << offset << ", length = " << length;

    DoWrite(mdsclient, fileInfo, fEpoch);
}

void IOTracker::StartAioWrite(CurveAioContext* ctx, MDSClient* mdsclient,
                              const FInfo_t* fileInfo,
                              const FileEpoch* fEpoch) {
    aioctx_ = ctx;
    data_ = ctx->buf;
    offset_ = ctx->offset;
    length_ = ctx->length;
    type_ = OpType::WRITE;

    VLOG(3) << "aiowrite op, offset = " << ctx->offset
            << ", length = " << ctx->length;

    DoWrite(mdsclient, fileInfo, fEpoch);
}

void IOTracker::DoWrite(MDSClient* mdsclient, const FInfo_t* fileInfo,
                        const FileEpoch* fEpoch) {
    if (nullptr == data_) {
        ReturnOnFail();
        return;
    }

    switch (userDataType_) {
        case UserDataType::RawBuffer:
            writeData_.append_user_data(data_, length_,
                                        TrivialDeleter);
            break;
        case UserDataType::IOBuffer:
            writeData_ = *reinterpret_cast<const butil::IOBuf*>(data_);
            break;
    }

    int ret = Splitor::IO2ChunkRequests(this, mc_, &reqlist_, &writeData_,
                                        offset_, length_,
                                        mdsclient, fileInfo, fEpoch);
    if (ret == 0) {
        uint32_t subIoIndex = 0;

        reqcount_.store(reqlist_.size(), std::memory_order_release);
        std::for_each(reqlist_.begin(), reqlist_.end(), [&](RequestContext* r) {
            r->done_->SetFileMetric(fileMetric_);
            r->done_->SetIOManager(iomanager_);
            r->subIoIndex_ = subIoIndex++;
        });
        ret = scheduler_->ScheduleRequest(reqlist_);
    } else {
        LOG(ERROR) << "splitor write io failed, "
                   << "offset = " << offset_ << ", length = " << length_;
    }

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recycle resource!";
        ReturnOnFail();
    }
}

void IOTracker::ReadSnapChunk(const ChunkIDInfo &cinfo,
    uint64_t seq, const std::vector<uint64_t>& snaps,
    uint64_t offset, uint64_t len,
    char *buf, SnapCloneClosure* scc) {
    scc_    = scc;
    data_   = buf;
    offset_ = offset;
    length_ = len;
    type_   = OpType::READ_SNAP;

    int ret = -1;
    do {
        ret = Splitor::SingleChunkIO2ChunkRequests(
            this, mc_, &reqlist_, cinfo, nullptr, offset_, length_, seq, snaps);
        if (ret == 0) {
            PrepareReadIOBuffers(reqlist_.size());
            uint32_t subIoIndex = 0;
            reqcount_.store(reqlist_.size(), std::memory_order_release);

            for (auto& req : reqlist_) {
                req->subIoIndex_ = subIoIndex++;
            }

            ret = scheduler_->ScheduleRequest(reqlist_);
        }
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "split or schedule failed, return and recycle resource!";
        ReturnOnFail();
    }
}

void IOTracker::DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &cinfo,
    uint64_t correctedSeq) {
    type_ = OpType::DELETE_SNAP;

    int ret = -1;
    do {
        RequestContext* newreqNode = RequestContext::NewInitedRequestContext();
        if (newreqNode == nullptr) {
            break;
        }

        newreqNode->correctedSeq_ = correctedSeq;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "DeleteSnapChunkOrCorrectSn request schedule failed,"
                   << "return and recycle resource!";
        ReturnOnFail();
    }
}

void IOTracker::GetChunkInfo(const ChunkIDInfo &cinfo,
    ChunkInfoDetail *chunkInfo) {
    type_ = OpType::GET_CHUNK_INFO;

    int ret = -1;
    do {
        RequestContext* newreqNode = RequestContext::NewInitedRequestContext();
        if (newreqNode == nullptr) {
            break;
        }

        newreqNode->chunkinfodetail_ = chunkInfo;
        FillCommonFields(cinfo, newreqNode);

        reqlist_.push_back(newreqNode);
        reqcount_.store(reqlist_.size(), std::memory_order_release);

        ret = scheduler_->ScheduleRequest(reqlist_);
    } while (false);

    if (ret == -1) {
        LOG(ERROR) << "GetChunkInfo request schedule failed,"
                   << " return and recycle resource!";
        ReturnOnFail();
    }
}

void IOTracker::CreateCloneChunk(const std::string& location,
                                 const ChunkIDInfo& cinfo, uint64_t sn,
                                 uint64_t correntSn, uint64_t chunkSize,
                                 SnapCloneClosure* scc) {
    type_ = OpType::CREATE_CLONE;
    scc_ = scc;

    int ret = -1;
    do {
        RequestContext* newreqNode = RequestContext::NewInitedRequestContext();
        if (newreqNode == nullptr) {
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
        LOG(ERROR) << "CreateCloneChunk request schedule failed,"
                   << "return and recycle resource!";
        ReturnOnFail();
    }
}

void IOTracker::RecoverChunk(const ChunkIDInfo& cinfo, uint64_t offset,
                             uint64_t len, SnapCloneClosure* scc) {
    type_ = OpType::RECOVER_CHUNK;
    scc_ = scc;

    int ret = -1;
    do {
        RequestContext* newreqNode = RequestContext::NewInitedRequestContext();
        if (newreqNode == nullptr) {
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
        LOG(ERROR) << "RecoverChunk request schedule failed,"
                   << " return and recycle resource!";
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
        ChunkServerErr2LibcurveErr(static_cast<CHUNK_OP_STATUS>(errorcode),
                                   &errcode_);
    }

    // copy read data
    if (OpType::READ == type_ || OpType::READ_SNAP == type_) {
        SetReadData(reqctx->subIoIndex_, reqctx->readData_);
    }

    if (1 == reqcount_.fetch_sub(1, std::memory_order_acq_rel)) {
        Done();
    }
}

int IOTracker::Wait() {
    return iocv_.Wait();
}

void IOTracker::Done() {
    if (errcode_ == LIBCURVE_ERROR::OK) {
        uint64_t duration = TimeUtility::GetTimeofDayUs() - opStartTimePoint_;
        MetricHelper::UserLatencyRecord(fileMetric_, duration, type_);
        MetricHelper::IncremUserQPSCount(fileMetric_, length_, type_);

        // copy read data to user buffer
        if (OpType::READ == type_ || OpType::READ_SNAP == type_) {
            butil::IOBuf readData;
            for (const auto& buf : readDatas_) {
                readData.append(buf);
            }

            switch (userDataType_) {
                case UserDataType::RawBuffer: {
                    size_t nc = readData.copy_to(data_, readData.size());
                    if (nc != length_) {
                        errcode_ = LIBCURVE_ERROR::FAILED;
                    }
                    break;
                }
                case UserDataType::IOBuffer: {
                    butil::IOBuf* userData =
                        reinterpret_cast<butil::IOBuf*>(data_);
                    *userData = readData;
                    if (userData->size() != length_) {
                        errcode_ = LIBCURVE_ERROR::FAILED;
                    }
                    break;
                }
            }

            if (errcode_ != LIBCURVE_ERROR::OK) {
                LOG(ERROR) << "IO Error, copy data to read buffer failed, "
                           << ", filename: " << fileMetric_->filename
                           << ", offset: " << offset_
                           << ", length: " << length_;
            }
        }
    } else {
        MetricHelper::IncremUserEPSCount(fileMetric_, type_);
        if (type_ == OpType::READ || type_ == OpType::WRITE) {
            if (LIBCURVE_ERROR::EPOCH_TOO_OLD == errcode_) {
                LOG(WARNING) << "file [" << fileMetric_->filename << "]"
                        << ", epoch too old, OpType = " << OpTypeToString(type_)
                        << ", offset = " << offset_
                        << ", length = " << length_;
            } else {
                LOG(ERROR) << "file [" << fileMetric_->filename << "]"
                        << ", IO Error, OpType = " << OpTypeToString(type_)
                        << ", offset = " << offset_
                        << ", length = " << length_;
            }
        } else {
            if (OpType::CREATE_CLONE == type_ &&
                LIBCURVE_ERROR::EXISTS == errcode_) {
                // if CreateCloneChunk return Exists
                // no error should be reported
            } else {
                LOG(ERROR) << "IO Error, OpType = " << OpTypeToString(type_);
            }
        }
    }

    DestoryRequestList();

    // scc_和aioctx都为空的时候肯定是个同步调用
    if (scc_ == nullptr && aioctx_ == nullptr) {
        errcode_ == LIBCURVE_ERROR::OK ? iocv_.Complete(length_)
                                       : iocv_.Complete(-errcode_);
        return;
    }

    // 异步函数调用，在此处发起回调
    if (aioctx_ != nullptr) {
        aioctx_->ret = errcode_ == LIBCURVE_ERROR::OK ? length_ : -errcode_;
        aioctx_->cb(aioctx_);
    } else {
        int ret = errcode_ == LIBCURVE_ERROR::OK ? length_ : -errcode_;
        scc_->SetRetCode(ret);
        scc_->Run();
    }

    // 回收当前io tracker
    iomanager_->HandleAsyncIOResponse(this);
}

void IOTracker::DestoryRequestList() {
    for (auto iter : reqlist_) {
        iter->UnInit();
        delete iter;
    }
}

void IOTracker::ReturnOnFail() {
    errcode_ = LIBCURVE_ERROR::FAILED;
    Done();
}

void IOTracker::ChunkServerErr2LibcurveErr(CHUNK_OP_STATUS errcode,
    LIBCURVE_ERROR* errout) {
    switch (errcode) {
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            *errout = LIBCURVE_ERROR::OK;
            break;
        // chunk或者copyset对于用户来说是透明的，所以直接返回错误
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST:
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST:
            *errout = LIBCURVE_ERROR::NOTEXIST;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CRC_FAIL:
            *errout = LIBCURVE_ERROR::CRC_ERROR;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST:
            *errout = LIBCURVE_ERROR::INVALID_REQUEST;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_DISK_FAIL:
            *errout = LIBCURVE_ERROR::DISK_FAIL;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_NOSPACE:
            *errout = LIBCURVE_ERROR::NO_SPACE;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_EXIST:
            *errout = LIBCURVE_ERROR::EXISTS;
            break;
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD:
            *errout = LIBCURVE_ERROR::EPOCH_TOO_OLD;
            break;
        default:
            *errout = LIBCURVE_ERROR::FAILED;
            break;
    }
}

}   // namespace client
}   // namespace curve
