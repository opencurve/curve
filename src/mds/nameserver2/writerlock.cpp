/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2022-07-25
 * Author: YangFan (fansehep)
 */

#include "src/mds/nameserver2/writerlock.h"
#include "proto/nameserver2.pb.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {

using ::curve::common::TimeUtility;
using ::curve::common::NameLockGuard;
using ::curve::common::EncodeBigEndian;
using ::curve::common::TimeUtility;

WriterStatus WriterLock::GetWriterStatus(const uint64_t fileid,
                                         std::string* clientuuid) {
    WriterLockInfo info;
    auto errcode = storage_->GetFilePermInfo(
        fileid, &info);
    if (errcode == StoreStatus::KeyNotExist) {
        return WriterStatus::kNoExist;
    } else if (errcode == StoreStatus::InternalError) {
        return WriterStatus::kError;
    }
    if (clientuuid) {
        *clientuuid = info.clientuuid();
    }
    auto now_date = TimeUtility::GetTimeofDayMs();
    if ((now_date - info.lastreceive()) >= (timeoutopt_.timeoutus / 1000)) {
        return WriterStatus::kExpired;
    }
    return WriterStatus::kSuccess;
}

bool WriterLock::SetWriter(const uint64_t fileid,
                           const std::string& clientuuid) {
    WriterLockInfo info;
    info.set_clientuuid(clientuuid);
    info.set_lastreceive(TimeUtility::GetTimeofDayMs());
    auto errcode = storage_->PutFilePermInfo(fileid, info);
    if (errcode == StoreStatus::OK) {
        return true;
    }
    return false;
}

bool WriterLock::Lock(const uint64_t fileid,
                      const std::string& clientuuid) {
    NameLockGuard lg(namelock_, std::to_string(fileid));
    auto curwriterstatus = GetWriterStatus(fileid, nullptr);
    if (curwriterstatus == WriterStatus::kSuccess ||
        curwriterstatus == WriterStatus::kError) {
        LOG(INFO) << " fileid = " << fileid << " clientuuid = "
            << clientuuid << " be reader";
        return false;
    } else {
        LOG(INFO) << " filename = " << fileid << " clientuuid = "
            << clientuuid << " lock the file"
            << " kouttime = " << timeoutopt_.timeoutus << "us";
        SetWriter(fileid, clientuuid);
        return true;
    }
}

bool WriterLock::Unlock(const uint64_t fileid,
                        const std::string& clientuuid) {
    NameLockGuard lg(namelock_, std::to_string(fileid));
    std::string curwriteruuid;
    auto curwriterstatus = GetWriterStatus(fileid, &curwriteruuid);
    if (curwriterstatus == WriterStatus::kError ||
        curwriterstatus == WriterStatus::kNoExist ||
        curwriterstatus == WriterStatus::kExpired) {
        return false;
    } else {
        if (curwriteruuid != clientuuid) {
            return false;
        } else {
            ClearWriter(fileid);
            return true;
        }
    }
    return false;
}

bool WriterLock::ClearWriter(const uint64_t fileid) {
    auto err = storage_->ClearPermInfo(fileid);
    if (err == StoreStatus::OK) {
        return true;
    }
    return false;
}

bool WriterLock::UpdateLockTime(const uint64_t fileid,
                                const std::string& clientuuid) {
    NameLockGuard lg(namelock_, std::to_string(fileid));
    std::string curwriteruuid;
    auto curstatus = GetWriterStatus(fileid, &curwriteruuid);
    if (curstatus == WriterStatus::kError ||
        curstatus == WriterStatus::kNoExist) {
        LOG(ERROR) << "fileid = " << fileid << " clientuuid = "
            << clientuuid << "update error!";
        return false;
    }

    if (curstatus == WriterStatus::kSuccess &&
        curwriteruuid != clientuuid) {
        LOG(ERROR) << "fileid = " << fileid << " clientuuid = "
            << clientuuid << "is not current writer";
        return false;
    }
    LOG(INFO) << "fileid = " << fileid << " clientuuid = "
        << clientuuid << " updatelocktime";
    return SetWriter(fileid, clientuuid);
}

}    //  namespace mds
}    //  namespace curve
