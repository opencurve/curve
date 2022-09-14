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

/*************************************************************************
> File Name: snapshot_meta_store.cpp
> Author:
> Created Time: Mon Dec 17 13:47:19 2018
 ************************************************************************/
#include "src/snapshotcloneserver/common/snapshotclone_info.h"
#include <gflags/gflags.h>
#include <memory>
#include <glog/logging.h> //NOLINT

#include "proto/snapshotcloneserver.pb.h"

namespace curve {
namespace snapshotcloneserver {

bool CloneInfo::SerializeToString(std::string *value) const {
    CloneInfoData data;
    data.set_uuid(taskId_);
    data.set_user(user_);
    data.set_tasktype(static_cast<int>(type_));
    data.set_source(source_);
    data.set_destination(destination_);
    data.set_originid(originId_);
    data.set_destinationid(destinationId_);
    data.set_time(time_);
    data.set_filetype(static_cast<int>(fileType_));
    data.set_islazy(isLazy_);
    data.set_nextstep(static_cast<int>(nextStep_));
    data.set_status(static_cast<int>(status_));
    data.set_poolset(poolset_);
    return data.SerializeToString(value);
}

bool CloneInfo::ParseFromString(const std::string &value) {
    CloneInfoData data;
    bool ret = data.ParseFromString(value);
    taskId_ = data.uuid();
    user_ = data.user();
    type_ = static_cast<CloneTaskType>(data.tasktype());
    source_ = data.source();
    destination_ = data.destination();
    originId_ = data.originid();
    destinationId_ = data.destinationid();
    time_ = data.time();
    fileType_ = static_cast<CloneFileType>(data.filetype());
    isLazy_ = data.islazy();
    nextStep_ = static_cast<CloneStep>(data.nextstep());
    status_ = static_cast<CloneStatus>(data.status());
    poolset_ = data.poolset();
    return ret;
}

std::ostream& operator<<(std::ostream& os, const CloneInfo &cloneInfo) {
    os << "{ taskId : " << cloneInfo.GetTaskId();
    os << ", user : " << cloneInfo.GetUser();
    os << ", cloneTaskType : "
       << static_cast<int> (cloneInfo.GetTaskType());
    os << ", source : " << cloneInfo.GetSrc();
    os << ", destination : " << cloneInfo.GetDest();
    os << ", poolset : " << cloneInfo.GetPoolset();
    os << ", originId : " << cloneInfo.GetOriginId();
    os << ", destId : " << cloneInfo.GetDestId();
    os << ", time : " << cloneInfo.GetTime();
    os << ", fileType : " << static_cast<int>(cloneInfo.GetFileType());
    os << ", isLazy : " << cloneInfo.GetIsLazy();
    os << ", nextStep : " << static_cast<int>(cloneInfo.GetNextStep());
    os << ", status : " << static_cast<int>(cloneInfo.GetStatus());
    os << " }";
    return os;
}

bool SnapshotInfo::SerializeToString(std::string *value) const {
    SnapshotInfoData data;
    data.set_uuid(uuid_);
    data.set_user(user_);
    data.set_filename(fileName_);
    data.set_snapshotname(snapshotName_);
    data.set_seqnum(seqNum_);
    data.set_chunksize(chunkSize_);
    data.set_segmentsize(segmentSize_);
    data.set_filelength(fileLength_);
    data.set_stripeunit(stripeUnit_);
    data.set_stripecount(stripeCount_);
    data.set_poolset(poolset_);
    data.set_time(time_);
    data.set_status(static_cast<int>(status_));
    return data.SerializeToString(value);
}

bool SnapshotInfo::ParseFromString(const std::string &value) {
    SnapshotInfoData data;
    bool ret = data.ParseFromString(value);
    uuid_ = data.uuid();
    user_ = data.user();
    fileName_ = data.filename();
    snapshotName_ = data.snapshotname();
    seqNum_ = data.seqnum();
    chunkSize_ = data.chunksize();
    segmentSize_ = data.segmentsize();
    fileLength_ = data.filelength();
    if (data.has_stripeunit()) {
        stripeUnit_ = data.stripeunit();
    } else {
        stripeUnit_ = 0;
    }
    if (data.has_stripecount()) {
        stripeCount_ = data.stripecount();
    } else {
        stripeCount_ = 0;
    }
    poolset_ = data.poolset();
    time_ = data.time();
    status_ = static_cast<Status>(data.status());
    return ret;
}

std::ostream& operator<<(std::ostream& os, const SnapshotInfo &snapshotInfo) {
    os << "{ uuid : " << snapshotInfo.GetUuid();
    os << ", user : " << snapshotInfo.GetUser();
    os << ", fileName : " << snapshotInfo.GetFileName();
    os << ", snapshotName : " << snapshotInfo.GetSnapshotName();
    os << ", seqNum : " << snapshotInfo.GetSeqNum();
    os << ", chunkSize : " << snapshotInfo.GetChunkSize();
    os << ", segementSize : " << snapshotInfo.GetSegmentSize();
    os << ", fileLength : " << snapshotInfo.GetFileLength();
    os << ", stripeUnit :" << snapshotInfo.GetStripeUnit();
    os << ", stripeCount :" << snapshotInfo.GetStripeCount();
    os << ", poolset: " << snapshotInfo.GetPoolset();
    os << ", time : " << snapshotInfo.GetCreateTime();
    os << ", status : " << static_cast<int>(snapshotInfo.GetStatus());
    os << " }";
    return os;
}

}  // namespace snapshotcloneserver
}  // namespace curve
