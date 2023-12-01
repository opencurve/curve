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
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 */

#include "test/integration/snapshotcloneserver/fake_curvefs_client.h"

#include <fiu-control.h>
#include <fiu.h>

namespace curve {
namespace snapshotcloneserver {

const char* testUser1 = "user1";
const char* testFile1 = "/user1/testFile1";
const char* shortTestFile1Name = "testFile1";

const uint64_t chunkSize = 16ULL * 1024 * 1024;
const uint64_t segmentSize = 32ULL * 1024 * 1024;
const uint64_t fileLength = 64ULL * 1024 * 1024;

int FakeCurveFsClient::Init(const CurveClientOptions& options) {
    // Initialize a file for snapshot and cloning
    FInfo fileInfo;
    fileInfo.id = 100;
    fileInfo.parentid = 3;
    fileInfo.filetype = FileType::INODE_PAGEFILE;
    fileInfo.chunksize = chunkSize;
    fileInfo.segmentsize = segmentSize;
    fileInfo.length = fileLength;
    fileInfo.ctime = 100;
    fileInfo.seqnum = 1;
    fileInfo.owner = testUser1;
    fileInfo.filename = shortTestFile1Name;
    fileInfo.fullPathName = testFile1;
    fileInfo.filestatus = FileStatus::Created;
    fileInfo.poolset = "ssdPoolset1";

    fileMap_.emplace(testFile1, fileInfo);

    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::UnInit() { return LIBCURVE_ERROR::OK; }

int FakeCurveFsClient::CreateSnapshot(const std::string& filename,
                                      const std::string& user, uint64_t* seq) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.CreateSnapshot",
        -LIBCURVE_ERROR::FAILED);  // NOLINT

    auto it = fileMap_.find(filename);
    if (it != fileMap_.end()) {
        *seq = it->second.seqnum;

        FInfo snapInfo = it->second;
        snapInfo.filetype = FileType::INODE_SNAPSHOT_PAGEFILE;
        snapInfo.id = fileId_++;
        snapInfo.parentid = it->second.id;
        snapInfo.filename =
            (it->second.filename + "-" + std::to_string(it->second.seqnum));
        snapInfo.filestatus = FileStatus::Created;

        it->second.seqnum++;
        fileSnapInfoMap_.emplace(filename, snapInfo);
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::FAILED;
    }
}

int FakeCurveFsClient::DeleteSnapshot(const std::string& filename,
                                      const std::string& user, uint64_t seq) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.DeleteSnapshot",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileSnapInfoMap_.find(filename);
    if (it != fileSnapInfoMap_.end()) {
        fileSnapInfoMap_.erase(it);
        return LIBCURVE_ERROR::OK;
    }
    return -LIBCURVE_ERROR::NOTEXIST;
}

int FakeCurveFsClient::GetSnapshot(const std::string& filename,
                                   const std::string& user, uint64_t seq,
                                   FInfo* snapInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.GetSnapshot",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    if (fileSnapInfoMap_.find(filename) != fileSnapInfoMap_.end()) {
        *snapInfo = fileSnapInfoMap_[filename];
        return LIBCURVE_ERROR::OK;
    }
    return -LIBCURVE_ERROR::NOTEXIST;
}

int FakeCurveFsClient::GetSnapshotSegmentInfo(const std::string& filename,
                                              const std::string& user,
                                              uint64_t seq, uint64_t offset,
                                              SegmentInfo* segInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.GetSnapshotSegmentInfo",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    segInfo->segmentsize = segmentSize;
    segInfo->chunksize = chunkSize;
    segInfo->startoffset = offset;
    // 2 segments in total
    if (offset == 0) {
        segInfo->chunkvec = {{1, 1, 1}, {2, 2, 1}};
    } else {
        segInfo->chunkvec = {{3, 3, 1}, {4, 4, 1}};
    }
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::ReadChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq,
                                         uint64_t offset, uint64_t len,
                                         char* buf, SnapCloneClosure* scc) {
    scc->SetRetCode(LIBCURVE_ERROR::OK);
    scc->Run();
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.ReadChunkSnapshot",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    memset(buf, 'x', len);
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::CheckSnapShotStatus(std::string filename,
                                           std::string user, uint64_t seq,
                                           FileStatus* filestatus) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.CheckSnapShotStatus",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    return -LIBCURVE_ERROR::NOTEXIST;
}

int FakeCurveFsClient::GetChunkInfo(const ChunkIDInfo& cidinfo,
                                    ChunkInfoDetail* chunkInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.GetChunkInfo",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    chunkInfo->chunkSn.push_back(1);
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::CreateCloneFile(
    const std::string& source, const std::string& filename,
    const std::string& user, uint64_t size, uint64_t sn, uint32_t chunkSize,
    uint64_t stripeUnit, uint64_t stripeCount, const std::string& poolset,
    FInfo* fileInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.CreateCloneFile",
        -LIBCURVE_ERROR::FAILED);  // NOLINT

    fileInfo->id = fileId_++;
    fileInfo->parentid = 2;
    fileInfo->filetype = FileType::INODE_PAGEFILE;
    fileInfo->chunksize = chunkSize;
    fileInfo->segmentsize = segmentSize;
    fileInfo->length = fileLength;
    fileInfo->ctime = 100;
    fileInfo->seqnum = sn;
    fileInfo->userinfo = UserInfo_t(user, "");
    fileInfo->owner = user;
    fileInfo->filename = filename;
    fileInfo->fullPathName = filename;
    fileInfo->filestatus = FileStatus::Cloning;
    fileInfo->stripeUnit = stripeUnit;
    fileInfo->stripeCount = stripeCount;
    fileInfo->poolset = poolset;

    LOG(INFO) << "CreateCloneFile " << filename;
    fileMap_.emplace(filename, *fileInfo);

    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::CreateCloneChunk(const std::string& location,
                                        const ChunkIDInfo& chunkidinfo,
                                        uint64_t sn, uint64_t csn,
                                        uint64_t chunkSize,
                                        SnapCloneClosure* scc) {
    scc->SetRetCode(LIBCURVE_ERROR::OK);
    scc->Run();
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.CreateCloneChunk",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::RecoverChunk(const ChunkIDInfo& chunkidinfo,
                                    uint64_t offset, uint64_t len,
                                    SnapCloneClosure* scc) {
    scc->SetRetCode(LIBCURVE_ERROR::OK);
    scc->Run();
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::CompleteCloneMeta(const std::string& filename,
                                         const std::string& user) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.CompleteCloneMeta",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileMap_.find(filename);
    if (it != fileMap_.end()) {
        it->second.filestatus = FileStatus::CloneMetaInstalled;
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

int FakeCurveFsClient::CompleteCloneFile(const std::string& filename,
                                         const std::string& user) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.CompleteCloneFile",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileMap_.find(filename);
    if (it != fileMap_.end()) {
        it->second.filestatus = FileStatus::Cloned;
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

int FakeCurveFsClient::SetCloneFileStatus(const std::string& filename,
                                          const FileStatus& filestatus,
                                          const std::string& user) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.SetCloneFileStatus",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileMap_.find(filename);
    if (it != fileMap_.end()) {
        it->second.filestatus = filestatus;
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

int FakeCurveFsClient::GetFileInfo(const std::string& filename,
                                   const std::string& user, FInfo* fileInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    if (fileMap_.find(filename) != fileMap_.end()) {
        *fileInfo = fileMap_[filename];
        return LIBCURVE_ERROR::OK;
    }
    return -LIBCURVE_ERROR::NOTEXIST;
}

int FakeCurveFsClient::GetOrAllocateSegmentInfo(bool allocate, uint64_t offset,
                                                FInfo* fileInfo,
                                                const std::string& user,
                                                SegmentInfo* segInfo) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.GetOrAllocateSegmentInfo",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    segInfo->segmentsize = segmentSize;
    segInfo->chunksize = chunkSize;
    segInfo->startoffset = offset;
    // 2 segments in total
    if (offset == 0) {
        segInfo->chunkvec = {{1, 1, 1}, {2, 2, 1}};
    } else {
        segInfo->chunkvec = {{3, 3, 1}, {4, 4, 1}};
    }
    return LIBCURVE_ERROR::OK;
}

int FakeCurveFsClient::RenameCloneFile(const std::string& user,
                                       uint64_t originId,
                                       uint64_t destinationId,
                                       const std::string& origin,
                                       const std::string& destination) {
    LOG(INFO) << "RenameCloneFile from " << origin << " to " << destination;
    fiu_return_on(
        "test/integration/snapshotcloneserver/"
        "FakeCurveFsClient.RenameCloneFile",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileMap_.find(origin);
    if (it != fileMap_.end()) {
        it->second.parentid = 3;
        it->second.filename = destination;
        fileMap_[destination] = it->second;
        fileMap_.erase(it);
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

int FakeCurveFsClient::DeleteFile(const std::string& fileName,
                                  const std::string& user, uint64_t fileId) {
    auto it = fileMap_.find(fileName);
    if (it != fileMap_.end()) {
        fileMap_.erase(it);
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

int FakeCurveFsClient::Mkdir(const std::string& dirpath,
                             const std::string& user) {
    return -LIBCURVE_ERROR::EXISTS;
}

int FakeCurveFsClient::ChangeOwner(const std::string& filename,
                                   const std::string& newOwner) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner",
        -LIBCURVE_ERROR::FAILED);  // NOLINT
    auto it = fileMap_.find(filename);
    if (it != fileMap_.end()) {
        it->second.owner = newOwner;
        return LIBCURVE_ERROR::OK;
    } else {
        return -LIBCURVE_ERROR::NOTEXIST;
    }
}

bool FakeCurveFsClient::JudgeCloneDirHasFile() {
    for (auto& f : fileMap_) {
        if (2 == f.second.parentid) {
            LOG(INFO) << "Clone dir has file, fileinfo is :"
                      << " id = " << f.second.id
                      << ", owner = " << f.second.owner
                      << ", filename = " << f.second.filename
                      << ", fullPathName = " << f.second.fullPathName
                      << ", filestatus = "
                      << static_cast<int>(f.second.filestatus);
            return true;
        }
    }
    return false;
}

}  // namespace snapshotcloneserver
}  // namespace curve
