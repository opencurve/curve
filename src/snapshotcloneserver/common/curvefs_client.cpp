/*
 * Project: curve
 * Created Date: Mon Jan 07 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/common/curvefs_client.h"

using ::curve::client::LogicalPoolCopysetIDInfo;
using ::curve::client::UserInfo;

namespace curve {
namespace snapshotcloneserver {
int CurveFsClientImpl::Init(const CurveClientOptions &options) {
    mdsRootUser_ = options.mdsRootUser;
    mdsRootPassword_ = options.mdsRootPassword;

    if (client_.Init(options.configPath) < 0
        || fileClient_.Init(options.configPath) < 0) {
        return kErrCodeServerInitFail;
    }
    return kErrCodeSuccess;
}

int CurveFsClientImpl::UnInit() {
    client_.UnInit();
    return kErrCodeSuccess;
}

int CurveFsClientImpl::CreateSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t *seq) {
    if (user == mdsRootUser_) {
        return client_.CreateSnapShot(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_), seq);
    }
    return client_.CreateSnapShot(filename, UserInfo(user, ""), seq);
}

int CurveFsClientImpl::DeleteSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t seq) {
    if (user == mdsRootUser_) {
        return client_.DeleteSnapShot(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_), seq);
    }
    return client_.DeleteSnapShot(filename, UserInfo(user, ""), seq);
}

int CurveFsClientImpl::GetSnapshot(
    const std::string &filename,
    const std::string &user,
    uint64_t seq,
    FInfo* snapInfo) {
    if (user == mdsRootUser_) {
        return client_.GetSnapShot(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_), seq, snapInfo);
    }
    return client_.GetSnapShot(filename, UserInfo(user, ""), seq, snapInfo);
}

int CurveFsClientImpl::GetSnapshotSegmentInfo(const std::string &filename,
    const std::string &user,
    uint64_t seq,
    uint64_t offset,
    SegmentInfo *segInfo) {
    if (user == mdsRootUser_) {
        return client_.GetSnapshotSegmentInfo(
            filename, UserInfo(mdsRootUser_, mdsRootPassword_),
            seq, offset, segInfo);
    }
    return client_.GetSnapshotSegmentInfo(
        filename, UserInfo(user, ""), seq, offset, segInfo);
}

int CurveFsClientImpl::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf) {
    return client_.ReadChunkSnapshot(
        cidinfo, seq, offset, len, buf);
}

int CurveFsClientImpl::DeleteChunkSnapshotOrCorrectSn(
    const ChunkIDInfo &cidinfo,
    uint64_t correctedSeq) {
    return client_.DeleteChunkSnapshotOrCorrectSn(cidinfo, correctedSeq);
}

int CurveFsClientImpl::CheckSnapShotStatus(std::string filename,
                        std::string user,
                        uint64_t seq,
                        FileStatus* filestatus) {
    if (user == mdsRootUser_) {
        return client_.CheckSnapShotStatus(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_),
            seq, filestatus);
    }
    return client_.CheckSnapShotStatus(filename, UserInfo(user, ""),
                                       seq, filestatus);
}

int CurveFsClientImpl::GetChunkInfo(
    const ChunkIDInfo &cidinfo,
    ChunkInfoDetail *chunkInfo) {
    return client_.GetChunkInfo(cidinfo, chunkInfo);
}

int CurveFsClientImpl::CreateCloneFile(
    const std::string &filename,
    const std::string &user,
    uint64_t size,
    uint64_t sn,
    uint32_t chunkSize,
    FInfo* fileInfo) {
    if (user == mdsRootUser_) {
        return client_.CreateCloneFile(filename,
                UserInfo(mdsRootUser_, mdsRootPassword_), size,
                sn, chunkSize, fileInfo);
    }
    return client_.CreateCloneFile(filename,
            UserInfo(user, ""), size,
            sn, chunkSize, fileInfo);
}

int CurveFsClientImpl::CreateCloneChunk(
    const std::string &location,
    const ChunkIDInfo &chunkidinfo,
    uint64_t sn,
    uint64_t csn,
    uint64_t chunkSize) {
    return client_.CreateCloneChunk(location, chunkidinfo, sn, csn, chunkSize);
}

int CurveFsClientImpl::RecoverChunk(
    const ChunkIDInfo &chunkidinfo,
    uint64_t offset,
    uint64_t len) {
    return client_.RecoverChunk(chunkidinfo, offset, len);
}

int CurveFsClientImpl::CompleteCloneMeta(
    const std::string &filename,
    const std::string &user) {
    if (user == mdsRootUser_) {
        return client_.CompleteCloneMeta(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_));
    }
    return client_.CompleteCloneMeta(filename,
        UserInfo(user, ""));
}

int CurveFsClientImpl::CompleteCloneFile(
    const std::string &filename,
    const std::string &user) {
    if (user == mdsRootUser_) {
        return client_.CompleteCloneFile(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_));
    }
    return client_.CompleteCloneFile(filename,
        UserInfo(user, ""));
}

int CurveFsClientImpl::SetCloneFileStatus(
    const std::string &filename,
    const FileStatus& filestatus,
    const std::string &user) {
    if (user == mdsRootUser_) {
        return client_.SetCloneFileStatus(filename,
            filestatus,
            UserInfo(mdsRootUser_, mdsRootPassword_));
    }
    return client_.SetCloneFileStatus(filename,
            filestatus,
            UserInfo(user, ""));
}

int CurveFsClientImpl::GetFileInfo(
    const std::string &filename,
    const std::string &user,
    FInfo* fileInfo) {
    if (user == mdsRootUser_) {
        return client_.GetFileInfo(filename,
            UserInfo(mdsRootUser_, mdsRootPassword_), fileInfo);
    }
    return client_.GetFileInfo(filename,
        UserInfo(user, ""), fileInfo);
}

int CurveFsClientImpl::GetOrAllocateSegmentInfo(
    bool allocate,
    uint64_t offset,
    FInfo* fileInfo,
    const std::string &user,
    SegmentInfo *segInfo) {
    if (user == mdsRootUser_) {
        fileInfo->userinfo = UserInfo(mdsRootUser_, mdsRootPassword_);
        return client_.GetOrAllocateSegmentInfo(allocate, offset,
                                                fileInfo, segInfo);
    }

    fileInfo->userinfo = UserInfo(user, "");
    return client_.GetOrAllocateSegmentInfo(allocate, offset,
                                            fileInfo, segInfo);
}

int CurveFsClientImpl::RenameCloneFile(
    const std::string &user,
    uint64_t originId,
    uint64_t destinationId,
    const std::string &origin,
    const std::string &destination) {
    if (user == mdsRootUser_) {
        return client_.RenameCloneFile(
            UserInfo(mdsRootUser_, mdsRootPassword_), originId, destinationId,
                origin, destination);
    }
    return client_.RenameCloneFile(
        UserInfo(user, ""), originId, destinationId,
            origin, destination);
}

int CurveFsClientImpl::DeleteFile(
    const std::string &fileName,
    const std::string &user,
    uint64_t fileId) {
    if (user == mdsRootUser_) {
        return client_.DeleteFile(fileName,
            UserInfo(mdsRootUser_, mdsRootPassword_), fileId);
    }
    return client_.DeleteFile(fileName,
        UserInfo(user, ""), fileId);
}

int CurveFsClientImpl::Mkdir(const std::string& dirpath,
    const std::string &user) {
    if (user == mdsRootUser_) {
        return fileClient_.Mkdir(dirpath,
            UserInfo(mdsRootUser_, mdsRootPassword_));
    }
    return fileClient_.Mkdir(dirpath,
        UserInfo(user, ""));
}

int CurveFsClientImpl::ChangeOwner(const std::string& filename,
    const std::string& newOwner) {
    return fileClient_.ChangeOwner(filename, newOwner,
        UserInfo(mdsRootUser_, mdsRootPassword_));
}

}  // namespace snapshotcloneserver
}  // namespace curve

