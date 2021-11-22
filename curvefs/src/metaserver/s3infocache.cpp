/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-02
 * @Author: majie1
 */

#include "curvefs/src/metaserver/s3infocache.h"

#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace metaserver {

void S3InfoCache::UpdateRecent(uint64_t fsid) {
    if (pos_.find(fsid) != pos_.end()) {
        recent_.erase(pos_[fsid]);
    } else if (recent_.size() >= capacity_) {
        auto old = recent_.back();
        recent_.pop_back();
        cache_.erase(old);
        pos_.erase(old);
    }
    recent_.push_front(fsid);
    pos_[fsid] = recent_.begin();
}

S3Info S3InfoCache::Get(uint64_t fsid) {
    UpdateRecent(fsid);
    return cache_[fsid];
}

void S3InfoCache::Put(uint64_t fsid, const S3Info& s3info) {
    UpdateRecent(fsid);
    cache_[fsid] = s3info;
}

S3InfoCache::RequestStatusCode S3InfoCache::RequestS3Info(uint64_t fsid,
                                                          S3Info* s3info) {
    // send GetFsInfoRequest to mds
    curvefs::mds::GetFsInfoRequest request;
    curvefs::mds::GetFsInfoResponse response;
    request.set_fsid(fsid);
    brpc::Channel channel;
    if (channel.Init(mdsAddrs_[mdsIndex_].c_str(), NULL) != 0) {
        VLOG(6) << "s3infocache: " << metaserverAddr_.ip << ":"
                << metaserverAddr_.port << " Fail to init channel to MDS "
                << mdsAddrs_[mdsIndex_];
        return RequestStatusCode::RPCFAILURE;
    }

    curvefs::mds::MdsService_Stub stub(&channel);
    brpc::Controller cntl;
    stub.GetFsInfo(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == ETIMEDOUT ||
            cntl.ErrorCode() == brpc::ELOGOFF ||
            cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
            VLOG(6) << "s3infocache: current mds: " << mdsAddrs_[mdsIndex_]
                    << " is shutdown or going to quit";
            mdsIndex_ = (mdsIndex_ + 1) % mdsAddrs_.size();
            VLOG(6) << "s3infocache: next heartbeat switch to "
                    << mdsAddrs_[mdsIndex_];
        } else {
            VLOG(6) << "s3infocache: " << metaserverAddr_.ip << ":"
                    << metaserverAddr_.port << " Fail to send heartbeat to MDS "
                    << mdsAddrs_[mdsIndex_] << ","
                    << " cntl errorCode: " << cntl.ErrorCode()
                    << " cntl error: " << cntl.ErrorText();
        }
        return RequestStatusCode::RPCFAILURE;
    } else {
        auto ret = response.statuscode();
        VLOG(6) << "s3infocache: rpc statuscode " << ret;
        if (ret == curvefs::mds::FSStatusCode::OK) {
            const curvefs::mds::FsDetail& fsdetail = response.fsinfo().detail();
            if (fsdetail.has_s3info()) {
                *s3info = fsdetail.s3info();
                return RequestStatusCode::SUCCESS;
            } else {
                VLOG(6) << "s3infocache: fsid " << fsid
                        << "doesn't have s3info " << ret;
                return RequestStatusCode::NOS3INFO;
            }
        } else {
            return RequestStatusCode::RPCFAILURE;
        }
    }
}

int S3InfoCache::GetS3Info(uint64_t fsid, S3Info* s3info) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (cache_.find(fsid) != cache_.end()) {
        *s3info = Get(fsid);
        return 0;
    } else {
        // request s3info from mds
        S3Info info;
        RequestStatusCode ret = RequestS3Info(fsid, &info);
        if (ret == RequestStatusCode::SUCCESS) {
            Put(fsid, info);
            *s3info = info;
            return 0;
        } else {
            return -1;
        }
    }
}

void S3InfoCache::InvalidateS3Info(uint64_t fsid) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (pos_.find(fsid) != pos_.end()) {
        auto iter = pos_[fsid];
        recent_.erase(iter);
        pos_.erase(fsid);
        cache_.erase(fsid);
    }
}

}  // namespace metaserver
}  // namespace curvefs
