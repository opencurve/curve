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
 * Created Date: Mon Jan 07 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/common/curvefs_client.h"

using ::curve::client::LogicalPoolCopysetIDInfo;
using ::curve::client::UserInfo;

namespace curve {
namespace snapshotcloneserver {
int CurveFsClientImpl::Init(const CurveClientOptions &options) {
    mdsRootUser_ = options.mdsRootUser;
    mdsRootPassword_ = options.mdsRootPassword;
    clientMethodRetryTimeSec_ = options.clientMethodRetryTimeSec;
    clientMethodRetryIntervalMs_ = options.clientMethodRetryIntervalMs;

    if (snapClient_->Init(options.configPath) < 0) {
        return kErrCodeServerInitFail;
    } else if (fileClient_->Init(options.configPath) < 0) {
        snapClient_->UnInit();
        return kErrCodeServerInitFail;
    }
    return kErrCodeSuccess;
}

int CurveFsClientImpl::UnInit() {
    snapClient_->UnInit();
    fileClient_->UnInit();
    return kErrCodeSuccess;
}

int CurveFsClientImpl::CreateSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t *seq) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, &userInfo, seq] () {
            return snapClient_->CreateSnapShot(filename,
                userInfo, seq);
        };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
            ret != -LIBCURVE_ERROR::UNDER_SNAPSHOT &&
            ret != -LIBCURVE_ERROR::CLIENT_NOT_SUPPORT_SNAPSHOT;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::DeleteSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t seq) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, &userInfo, seq] () {
            return snapClient_->DeleteSnapShot(filename, userInfo, seq);
        };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
        ret != -LIBCURVE_ERROR::NOTEXIST &&
        ret != -LIBCURVE_ERROR::DELETING;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::GetSnapshot(
    const std::string &filename,
    const std::string &user,
    uint64_t seq,
    FInfo* snapInfo) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, &userInfo, seq, snapInfo] () {
        return snapClient_->GetSnapShot(filename,
            userInfo, seq, snapInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::GetSnapshotSegmentInfo(const std::string &filename,
    const std::string &user,
    uint64_t seq,
    uint64_t offset,
    SegmentInfo *segInfo) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, &userInfo, seq, offset, segInfo] () {
        return snapClient_->GetSnapshotSegmentInfo(
            filename, userInfo,
            seq, offset, segInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOT_ALLOCATE;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf,
                        SnapCloneClosure* scc) {
    RetryMethod method = [this, &cidinfo, seq, offset, len, buf, scc] () {
        return snapClient_->ReadChunkSnapshot(
            cidinfo, seq, {seq}, offset, len, buf, scc);
    };
    RetryCondition condition = [] (int ret) {
        return ret < 0;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::CheckSnapShotStatus(std::string filename,
                        std::string user,
                        uint64_t seq,
                        FileStatus* filestatus,
                        uint32_t* progress) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method =
        [this, &filename, &userInfo, seq, filestatus, progress] () {
        return snapClient_->CheckSnapShotStatus(filename,
            userInfo,
            seq, filestatus, progress);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST &&
               ret != -LIBCURVE_ERROR::DELETE_ERROR;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::GetChunkInfo(
    const ChunkIDInfo &cidinfo,
    ChunkInfoDetail *chunkInfo) {
    RetryMethod method = [this, &cidinfo, &chunkInfo] () {
        return snapClient_->GetChunkInfo(cidinfo, chunkInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::CreateCloneFile(
    const std::string &source,
    const std::string &filename,
    const std::string &user,
    uint64_t size,
    uint64_t sn,
    uint32_t chunkSize,
    uint64_t stripeUnit,
    uint64_t stripeCount,
    const std::string& poolset,
    FInfo* fileInfo) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &source, &filename,
        userInfo, size, sn, chunkSize, stripeUnit, stripeCount, fileInfo,
        poolset] () {
            return snapClient_->CreateCloneFile(source, filename,
                userInfo, size,
                sn, chunkSize, stripeUnit, stripeCount, poolset, fileInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::EXISTS;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::CreateCloneChunk(
    const std::string &location,
    const ChunkIDInfo &chunkidinfo,
    uint64_t sn,
    uint64_t csn,
    uint64_t chunkSize,
    SnapCloneClosure* scc) {
    RetryMethod method = [this, &location, &chunkidinfo,
        sn, csn, chunkSize, scc] () {
        return snapClient_->CreateCloneChunk(
                location, chunkidinfo, sn, csn, chunkSize, scc);
    };
    RetryCondition condition = [] (int ret) {
        return ret < 0;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::RecoverChunk(
    const ChunkIDInfo &chunkidinfo,
    uint64_t offset,
    uint64_t len,
    SnapCloneClosure* scc) {
    RetryMethod method = [this, &chunkidinfo, offset, len, scc] () {
        return snapClient_->RecoverChunk(chunkidinfo, offset, len, scc);
    };
    RetryCondition condition = [] (int ret) {
        return ret < 0;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::CompleteCloneMeta(
    const std::string &filename,
    const std::string &user) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, userInfo] () {
        return snapClient_->CompleteCloneMeta(filename, userInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::CompleteCloneFile(
    const std::string &filename,
    const std::string &user) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, userInfo] () {
        return snapClient_->CompleteCloneFile(filename, userInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::SetCloneFileStatus(
    const std::string &filename,
    const FileStatus& filestatus,
    const std::string &user) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, filestatus, userInfo] () {
        return snapClient_->SetCloneFileStatus(filename, filestatus, userInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::GetFileInfo(
    const std::string &filename,
    const std::string &user,
    FInfo* fileInfo) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &filename, &userInfo, fileInfo] () {
        return snapClient_->GetFileInfo(filename,
            userInfo, fileInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST &&
               ret != -LIBCURVE_ERROR::AUTHFAIL &&
               ret != -LIBCURVE_ERROR::PARAM_ERROR;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::GetOrAllocateSegmentInfo(
    bool allocate,
    uint64_t offset,
    FInfo* fileInfo,
    const std::string &user,
    SegmentInfo *segInfo) {
    fileInfo->userinfo = GetUserInfo(user);
    RetryMethod method = [this, &allocate, offset,
        fileInfo, segInfo] () {
        return snapClient_->GetOrAllocateSegmentInfo(allocate, offset,
                                                fileInfo, segInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOT_ALLOCATE;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::RenameCloneFile(
    const std::string &user,
    uint64_t originId,
    uint64_t destinationId,
    const std::string &origin,
    const std::string &destination) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &userInfo, originId, destinationId,
        origin, destination] () {
        return snapClient_->RenameCloneFile(
            userInfo, originId, destinationId,
                origin, destination);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST &&
               ret != -LIBCURVE_ERROR::NOT_SUPPORT &&
               ret != -LIBCURVE_ERROR::FILE_OCCUPIED;  // file is in-use
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::DeleteFile(
    const std::string &fileName,
    const std::string &user,
    uint64_t fileId) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, fileName, userInfo, fileId] () {
        return snapClient_->DeleteFile(fileName,
            userInfo, fileId);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::NOTEXIST &&
               ret != -LIBCURVE_ERROR::NOT_SUPPORT &&
               ret != -LIBCURVE_ERROR::FILE_OCCUPIED;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::Mkdir(const std::string& dirpath,
    const std::string &user) {
    UserInfo userInfo = GetUserInfo(user);
    RetryMethod method = [this, &dirpath, userInfo] () {
        return fileClient_->Mkdir(dirpath, userInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
               ret != -LIBCURVE_ERROR::EXISTS;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

int CurveFsClientImpl::ChangeOwner(const std::string& filename,
    const std::string& newOwner) {
    UserInfo userInfo(mdsRootUser_, mdsRootPassword_);
    RetryMethod method = [this, &filename, &newOwner, &userInfo] () {
        return fileClient_->ChangeOwner(filename, newOwner, userInfo);
    };
    RetryCondition condition = [] (int ret) {
        return ret != LIBCURVE_ERROR::OK &&
            ret != LIBCURVE_ERROR::NOT_SUPPORT &&
            ret != LIBCURVE_ERROR::FILE_OCCUPIED;
    };
    RetryHelper retryHelper(method, condition);
    return retryHelper.RetryTimeSecAndReturn(clientMethodRetryTimeSec_,
        clientMethodRetryIntervalMs_);
}

}  // namespace snapshotcloneserver
}  // namespace curve
