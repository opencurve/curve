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
 * File Created: Monday, 18th February 2019 11:20:10 am
 * Author: tongguangxun
 */

#include "src/client/libcurve_snapshot.h"

#include <glog/logging.h>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"

namespace curve {
namespace client {
SnapshotClient::SnapshotClient() {}

int SnapshotClient::Init(const ClientConfigOption& clientopt) {
    google::SetCommandLineOption(
        "minloglevel", std::to_string(clientopt.loginfo.logLevel).c_str());
    int ret = -LIBCURVE_ERROR::FAILED;
    do {
        if (mdsclient_.Initialize(clientopt.metaServerOpt) !=
            LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "MDSClient init failed!";
            break;
        }

        if (!iomanager4chunk_.Initialize(clientopt.ioOpt, &mdsclient_)) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }
        ret = LIBCURVE_ERROR::OK;
    } while (0);

    return ret;
}

int SnapshotClient::Init(const std::string &configpath) {
    if (-1 == clientconfig_.Init(configpath)) {
        LOG(ERROR) << "config init failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    const auto& fileOpt = clientconfig_.GetFileServiceOption();
    ClientConfigOption opt;
    opt.loginfo = fileOpt.loginfo;
    opt.ioOpt = fileOpt.ioOpt;
    opt.commonOpt = fileOpt.commonOpt;
    opt.metaServerOpt = fileOpt.metaServerOpt;

    return Init(opt);
}

void SnapshotClient::UnInit() {
    iomanager4chunk_.UnInitialize();
    mdsclient_.UnInitialize();
}

int SnapshotClient::CreateSnapShot(const std::string &filename,
                                   const UserInfo_t &userinfo, uint64_t *seq) {
    LIBCURVE_ERROR ret = mdsclient_.CreateSnapShot(filename, userinfo, seq);
    return -ret;
}

int SnapshotClient::DeleteSnapShot(const std::string &filename,
                                   const UserInfo_t &userinfo, uint64_t seq) {
    LIBCURVE_ERROR ret = mdsclient_.DeleteSnapShot(filename, userinfo, seq);
    return -ret;
}

int SnapshotClient::GetSnapShot(const std::string &filename,
                                const UserInfo_t &userinfo, uint64_t seq,
                                FInfo *snapinfo) {
    std::map<uint64_t, FInfo> infomap;
    std::vector<uint64_t> seqvec;
    seqvec.push_back(seq);
    LIBCURVE_ERROR ret =
        mdsclient_.ListSnapShot(filename, userinfo, &seqvec, &infomap);
    if (ret == LIBCURVE_ERROR::OK && !infomap.empty()) {
        auto it = infomap.begin();
        if (it->first != seq) {
            LOG(WARNING) << "Snapshot info not found with seqnum = " << seq;
            return -LIBCURVE_ERROR::NOTEXIST;
        }
        *snapinfo = it->second;
    }
    return -ret;
}

int SnapshotClient::ListSnapShot(const std::string &filename,
                                 const UserInfo_t &userinfo,
                                 const std::vector<uint64_t> *seq,
                                 std::map<uint64_t, FInfo> *snapif) {
    LIBCURVE_ERROR ret =
        mdsclient_.ListSnapShot(filename, userinfo, seq, snapif);
    return -ret;
}

int SnapshotClient::GetSnapshotSegmentInfo(const std::string &filename,
                                           const UserInfo_t &userinfo,
                                           uint64_t seq, uint64_t offset,
                                           SegmentInfo *segInfo) {
    int ret = mdsclient_.GetSnapshotSegmentInfo(filename, userinfo, seq, offset,
                                                segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(WARNING) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return -ret;
    }

    for (const auto &iter : segInfo->chunkvec) {
        iomanager4chunk_.GetMetaCache()->UpdateChunkInfoByID(iter.cid_, iter);
    }

    return GetServerList(segInfo->lpcpIDInfo.lpid, segInfo->lpcpIDInfo.cpidVec);
}

int SnapshotClient::GetServerList(const LogicPoolID &lpid,
                                  const std::vector<CopysetID> &csid) {
    std::vector<CopysetInfo<ChunkServerID>> cpinfoVec;
    int ret = mdsclient_.GetServerList(lpid, csid, &cpinfoVec);

    for (auto iter : cpinfoVec) {
        iomanager4chunk_.GetMetaCache()->UpdateCopysetInfo(lpid, iter.cpid_,
                                                           iter);
    }
    return -ret;
}

int SnapshotClient::CheckSnapShotStatus(const std::string &filename,
                                        const UserInfo_t &userinfo,
                                        uint64_t seq, FileStatus *filestatus) {
    LIBCURVE_ERROR ret =
        mdsclient_.CheckSnapShotStatus(filename, userinfo, seq, filestatus);
    return -ret;
}

int SnapshotClient::CreateCloneFile(const std::string& source,
                                    const std::string& destination,
                                    const UserInfo_t& userinfo,
                                    uint64_t size,
                                    uint64_t sn,
                                    uint32_t chunksize,
                                    uint64_t stripeUnit,
                                    uint64_t stripeCount,
                                    const std::string& poolset,
                                    FInfo* finfo) {
    LIBCURVE_ERROR ret = mdsclient_.CreateCloneFile(
            source, destination, userinfo, size, sn, chunksize, stripeUnit,
            stripeCount, poolset, finfo);
    return -ret;
}

int SnapshotClient::GetFileInfo(const std::string &filename,
                                const UserInfo_t &userinfo, FInfo *fileInfo) {
    FileEpoch_t fEpoch;
    LIBCURVE_ERROR ret = mdsclient_.GetFileInfo(filename, userinfo,
                                                fileInfo, &fEpoch);
    return -ret;
}

int SnapshotClient::GetOrAllocateSegmentInfo(bool allocate, uint64_t offset,
                                             const FInfo_t *fi,
                                             SegmentInfo *segInfo) {
    int ret = mdsclient_.GetOrAllocateSegment(
        allocate, offset, fi, nullptr, segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(INFO) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return -ret;
    }

    // update metacache
    int count = 0;
    for (auto iter : segInfo->chunkvec) {
        uint64_t index =
            (segInfo->startoffset + count * fi->chunksize) / fi->chunksize;
        iomanager4chunk_.GetMetaCache()->UpdateChunkInfoByIndex(index, iter);
        ++count;
    }
    return GetServerList(segInfo->lpcpIDInfo.lpid, segInfo->lpcpIDInfo.cpidVec);
}

int SnapshotClient::RenameCloneFile(const UserInfo_t &userinfo,
                                    uint64_t originId, uint64_t destinationId,
                                    const std::string &origin,
                                    const std::string &destination) {
    LIBCURVE_ERROR ret = mdsclient_.RenameFile(userinfo, origin, destination,
                                               originId, destinationId);
    return -ret;
}

int SnapshotClient::CompleteCloneMeta(const std::string &destination,
                                      const UserInfo_t &userinfo) {
    LIBCURVE_ERROR ret = mdsclient_.CompleteCloneMeta(destination, userinfo);
    return -ret;
}

int SnapshotClient::CompleteCloneFile(const std::string &destination,
                                      const UserInfo_t &userinfo) {
    LIBCURVE_ERROR ret = mdsclient_.CompleteCloneFile(destination, userinfo);
    return -ret;
}

int SnapshotClient::SetCloneFileStatus(const std::string &filename,
                                       const FileStatus &filestatus,
                                       const UserInfo_t &userinfo,
                                       uint64_t fileID) {
    LIBCURVE_ERROR ret =
        mdsclient_.SetCloneFileStatus(filename, filestatus, userinfo, fileID);
    return -ret;
}

int SnapshotClient::DeleteFile(const std::string &filename,
                               const UserInfo_t &userinfo, uint64_t id) {
    LIBCURVE_ERROR ret = mdsclient_.DeleteFile(filename, userinfo, false, id);
    return -ret;
}

int SnapshotClient::CreateCloneChunk(const std::string &location,
                                     const ChunkIDInfo &chunkidinfo,
                                     uint64_t sn, uint64_t correntSn,
                                     uint64_t chunkSize,
                                     SnapCloneClosure *scc) {
    return iomanager4chunk_.CreateCloneChunk(location, chunkidinfo, sn,
                                             correntSn, chunkSize, scc);
}

int SnapshotClient::RecoverChunk(const ChunkIDInfo &chunkidinfo,
                                 uint64_t offset, uint64_t len,
                                 SnapCloneClosure *scc) {
    return iomanager4chunk_.RecoverChunk(chunkidinfo, offset, len, scc);
}

int SnapshotClient::ReadChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq,
                                      uint64_t offset, uint64_t len, char *buf,
                                      SnapCloneClosure *scc) {
    return iomanager4chunk_.ReadSnapChunk(cidinfo, seq, offset, len, buf, scc);
}

int SnapshotClient::DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
                                                   uint64_t correctedSeq) {
    return iomanager4chunk_.DeleteSnapChunkOrCorrectSn(cidinfo, correctedSeq);
}

int SnapshotClient::GetChunkInfo(ChunkIDInfo cidinfo,
                                 ChunkInfoDetail *chunkInfo) {
    return iomanager4chunk_.GetChunkInfo(cidinfo, chunkInfo);
}
}  // namespace client
}  // namespace curve
