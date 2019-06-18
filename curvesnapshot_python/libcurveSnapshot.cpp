/*
 * Project: curve
 * File Created: Thursday, 16th May 2019 3:46:05 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>

#include "curvesnapshot_python/libcurveSnapshot.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/client_config.h"
#include "src/client/libcurve_define.h"
#include "src/client/client_common.h"

using curve::client::UserInfo;
using curve::client::ClientConfig;
using curve::client::SnapshotClient;

bool globalinited = false;
SnapshotClient* globalSnapshotclient = nullptr;

int Init(const char* path) {
    if (globalinited) {
        return 0;
    }

    ClientConfig cc;
    if (-1 == cc.Init(path)) {
        LOG(ERROR) << "config init failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    FileServiceOption_t fileopt = cc.GetFileServiceOption();
    ClientConfigOption_t copt;
    copt.loginfo = fileopt.loginfo;
    copt.ioOpt = fileopt.ioOpt;
    copt.metaServerOpt = fileopt.metaServerOpt;

    if (globalSnapshotclient == nullptr) {
        globalSnapshotclient = new SnapshotClient();
        int ret = globalSnapshotclient->Init(copt);
        globalinited = ret == 0 ? true : false;
    }

    return globalinited ? 0 : -LIBCURVE_ERROR::FAILED;
}

void ChunkIDInfo2LocalInfo(CChunkIDInfo* localinfo,
                           const curve::client::ChunkIDInfo& idinfo) {
    localinfo->cid_.value = idinfo.cid_;
    localinfo->cpid_.value = idinfo.cpid_;
    localinfo->lpid_.value = idinfo.lpid_;
}

void LocalInfo2ChunkIDInfo(const CChunkIDInfo& localinfo,
                           curve::client::ChunkIDInfo* idinfo) {
    idinfo->cid_ = localinfo.cid_.value;
    idinfo->cpid_ = localinfo.cpid_.value;
    idinfo->lpid_ = localinfo.lpid_.value;
}

int CreateSnapShot(const char* filename,
                   const CUserInfo_t userinfo,
                   type_uInt64_t* seq) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }
    int ret = globalSnapshotclient->CreateSnapShot(
                                 filename,
                                 UserInfo(userinfo.owner, userinfo.password),
                                 &seq->value);
    LOG(ERROR) << "create snapshot ret = " << ret
               << ", seq = " << seq->value;
    return ret;
}

int DeleteSnapShot(const char* filename,
                   const CUserInfo_t userinfo,
                   type_uInt64_t seq) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return globalSnapshotclient->DeleteSnapShot(filename,
                                UserInfo(userinfo.owner, userinfo.password),
                                seq.value);
}

int GetSnapShot(const char* filename, const CUserInfo_t userinfo,
                type_uInt64_t seq, CFInfo_t* snapinfo) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }
    curve::client::FInfo_t fileinfo;

    int ret = globalSnapshotclient->GetSnapShot(filename,
                                UserInfo(userinfo.owner, userinfo.password),
                                seq.value,
                                &fileinfo);
    snapinfo->id.value = fileinfo.id;
    snapinfo->parentid.value = fileinfo.parentid;
    snapinfo->filetype = static_cast<CFileType>(fileinfo.filetype);
    snapinfo->chunksize.value = fileinfo.chunksize;
    snapinfo->segmentsize.value = fileinfo.segmentsize;
    snapinfo->length.value = fileinfo.length;
    snapinfo->ctime.value = fileinfo.ctime;
    snapinfo->seqnum.value = fileinfo.seqnum;
    memset(snapinfo->owner, 0, 256);
    memset(snapinfo->filename, 0, 256);
    memcpy(snapinfo->owner, fileinfo.owner.c_str(), 256);
    memcpy(snapinfo->filename, fileinfo.filename.c_str(), 256);
    snapinfo->filestatus = static_cast<CFileStatus>(fileinfo.filestatus);
    LOG(INFO) << "origin owner = " << fileinfo.owner;
    LOG(INFO) << "origin filename = " << fileinfo.filename;
    LOG(INFO) << "owner = " << snapinfo->owner;
    LOG(INFO) << "filename = " << snapinfo->filename;
    return ret;
}

int GetSnapshotSegmentInfo(const char* filename,
                        const CUserInfo_t userinfo,
                        type_uInt64_t seq,
                        type_uInt64_t offset,
                        CSegmentInfo *segInfo) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::SegmentInfo seg;
    int ret = globalSnapshotclient->GetSnapshotSegmentInfo(filename,
                                UserInfo(userinfo.owner, userinfo.password),
                                seq.value,
                                offset.value,
                                &seg);
    segInfo->segmentsize.value = seg.segmentsize;
    segInfo->chunksize.value = seg.chunksize;
    segInfo->startoffset.value = seg.startoffset;
    segInfo->chunkVecSize.value = seg.chunkvec.size();
    for (int i = 0; i < seg.chunkvec.size(); i++) {
        CChunkIDInfo_t tempIDInfo;
        ChunkIDInfo2LocalInfo(&tempIDInfo, seg.chunkvec[i]);
        segInfo->chunkvec.push_back(tempIDInfo);
    }
    segInfo->lpcpIDInfo.lpid.value = seg.lpcpIDInfo.lpid;
    segInfo->lpcpIDInfo.cpidVecSize.value = seg.lpcpIDInfo.cpidVec.size();
    for (int i = 0; i < seg.lpcpIDInfo.cpidVec.size(); i++) {
        segInfo->lpcpIDInfo.cpidVec.push_back(seg.lpcpIDInfo.cpidVec[i]);
    }
    return ret;
}

int ReadChunkSnapshot(CChunkIDInfo cidinfo,
                        type_uInt64_t seq,
                        type_uInt64_t offset,
                        type_uInt64_t len,
                        char *buf) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::ChunkIDInfo idinfo;
    LocalInfo2ChunkIDInfo(cidinfo, &idinfo);
    int ret = globalSnapshotclient->ReadChunkSnapshot(idinfo, seq.value,
                                                      offset.value, len.value,
                                                      buf);
    return ret;
}

int DeleteChunkSnapshotOrCorrectSn(CChunkIDInfo cidinfo,
                                   type_uInt64_t correctedSeq) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::ChunkIDInfo idinfo;
    LocalInfo2ChunkIDInfo(cidinfo, &idinfo);
    int ret = globalSnapshotclient->DeleteChunkSnapshotOrCorrectSn(idinfo,
                                                        correctedSeq.value);
    return ret;
}


int GetChunkInfo(CChunkIDInfo cidinfo, CChunkInfoDetail *chunkInfo) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::ChunkInfoDetail cinfodetail;
    curve::client::ChunkIDInfo idinfo;
    LocalInfo2ChunkIDInfo(cidinfo, &idinfo);

    int ret = globalSnapshotclient->GetChunkInfo(idinfo, &cinfodetail);
    chunkInfo->snSize.value = cinfodetail.chunkSn.size();
    for (int i = 0; i < cinfodetail.chunkSn.size(); i++) {
        chunkInfo->chunkSn.push_back(cinfodetail.chunkSn[i]);
    }
    return ret;
}


int CheckSnapShotStatus(const char* filename,
                            const CUserInfo_t userinfo,
                            type_uInt64_t seq,
                            type_uInt32_t* filestatus) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::FileStatus fs;
    int ret = globalSnapshotclient->CheckSnapShotStatus(filename,
                                UserInfo(userinfo.owner, userinfo.password),
                                seq.value,
                                &fs);
    filestatus->value = static_cast<uint32_t>(fs);
    return ret;
}


int CreateCloneChunk(const char* location,
                            const CChunkIDInfo chunkidinfo,
                            type_uInt64_t sn,
                            type_uInt64_t correntSn,
                            type_uInt64_t chunkSize) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::ChunkIDInfo idinfo;
    LocalInfo2ChunkIDInfo(chunkidinfo, &idinfo);

    int ret = globalSnapshotclient->CreateCloneChunk(location, idinfo,
                                                     sn.value, correntSn.value,
                                                     chunkSize.value);
    return ret;
}


int RecoverChunk(const CChunkIDInfo chunkidinfo,
                            type_uInt64_t offset,
                            type_uInt64_t len) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }
    curve::client::ChunkIDInfo idinfo;
    LocalInfo2ChunkIDInfo(chunkidinfo, &idinfo);

    int ret = globalSnapshotclient->RecoverChunk(idinfo,
                                                 offset.value,
                                                 len.value);
    return ret;
}

void UnInit() {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return;
    }
    globalSnapshotclient->UnInit();
}
