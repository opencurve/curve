/*
 * Project: curve
 * File Created: Thursday, 16th May 2019 3:46:05 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>

#include <string>
#include <memory>

#include "curvesnapshot_python/libcurveSnapshot.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/client_config.h"
#include "include/client/libcurve.h"
#include "src/client/client_common.h"

using curve::client::UserInfo;
using curve::client::ClientConfig;
using curve::client::SnapshotClient;
using curve::client::SnapCloneClosure;

class TaskTracker {
 public:
    TaskTracker()
    : concurrent_(0),
      lastErr_(0) {}

    /**
     * @brief 增加一个追踪任务
     */
    void AddOneTrace() {
        concurrent_.fetch_add(1, std::memory_order_acq_rel);
    }

    /**
     * @brief 获取任务数量
     *
     * @return 任务数量
     */
    uint32_t GetTaskNum() const {
        return concurrent_;
    }

    /**
     * @brief 处理任务返回值
     *
     * @param retCode 返回值
     */
    void HandleResponse(int retCode) {
        if (retCode < 0) {
            lastErr_ = retCode;
        }
        if (1 == concurrent_.fetch_sub(1, std::memory_order_acq_rel)) {
            // 最后一次需拿锁再发信号，防止先发信号后等待导致死锁
            std::unique_lock<Mutex> lk(cv_m);
            cv_.notify_all();
        } else {
            cv_.notify_all();
        }
    }

    /**
     * @brief 等待追踪的所有任务完成
     */
    void Wait() {
        std::unique_lock<Mutex> lk(cv_m);
        cv_.wait(lk, [this](){
            return concurrent_.load(std::memory_order_acquire) == 0;});
    }

    /**
     * @brief 获取最后一个错误
     *
     * @return 错误码
     */
    int GetResult() {
        return lastErr_;
    }

 private:
    // 等待的条件变量
    ConditionVariable cv_;
    Mutex cv_m;
    // 并发数量
    std::atomic<uint32_t> concurrent_;
    // 错误码
    int lastErr_;
};

struct SnapCloneTestClosure : public SnapCloneClosure {
    explicit SnapCloneTestClosure(std::shared_ptr<TaskTracker> tracker)
        : tracker_(tracker) {}
    void Run() {
        std::unique_ptr<SnapCloneTestClosure> self_guard(this);
        tracker_->HandleResponse(GetRetCode());
    }
    std::shared_ptr<TaskTracker> tracker_;
};

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
    LOG(INFO) << "create snapshot ret = " << ret
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
    if (ret == LIBCURVE_ERROR::OK) {
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
    }
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
    if (ret == LIBCURVE_ERROR::OK) {
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
    }
    return ret;
}

int GetOrAllocateSegmentInfo(const char* filename,
                            type_uInt64_t offset,
                            type_uInt64_t segmentsize,
                            type_uInt64_t chunksize,
                            const CUserInfo_t userinfo,
                            CSegmentInfo *segInfo) {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::FInfo_t fileinfo;
    fileinfo.segmentsize = segmentsize.value;
    fileinfo.chunksize = chunksize.value;
    fileinfo.fullPathName = std::string(filename);
    fileinfo.filename   = std::string(filename);
    fileinfo.userinfo = UserInfo(userinfo.owner, userinfo.password);

    curve::client::SegmentInfo seg;
    int ret = globalSnapshotclient->GetOrAllocateSegmentInfo(false,
                                offset.value,
                                &fileinfo,
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
    auto tracker = std::make_shared<TaskTracker>();
    SnapCloneTestClosure *cb = new SnapCloneTestClosure(tracker);

    tracker->AddOneTrace();
    int ret = globalSnapshotclient->ReadChunkSnapshot(idinfo, seq.value,
                                                      offset.value, len.value,
                                                      buf, cb);
    tracker->Wait();
    if (ret < 0) {
        return ret;
    } else {
        if (tracker->GetResult() < 0) {
            return tracker->GetResult();
        } else {
            return len.value;
        }
    }
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
    auto tracker = std::make_shared<TaskTracker>();
    SnapCloneTestClosure *cb = new SnapCloneTestClosure(tracker);

    tracker->AddOneTrace();
    int ret = globalSnapshotclient->CreateCloneChunk(location, idinfo,
                                                     sn.value, correntSn.value,
                                                     chunkSize.value,
                                                     cb);
    tracker->Wait();
    if (ret < 0) {
        return ret;
    } else {
        return tracker->GetResult();
    }
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
    auto tracker = std::make_shared<TaskTracker>();
    SnapCloneTestClosure *cb = new SnapCloneTestClosure(tracker);

    tracker->AddOneTrace();
    int ret = globalSnapshotclient->RecoverChunk(idinfo,
                                                 offset.value,
                                                 len.value,
                                                 cb);
    tracker->Wait();
    if (ret < 0) {
        return ret;
    } else {
        return tracker->GetResult();
    }
}

void UnInit() {
    if (globalSnapshotclient == nullptr) {
        LOG(ERROR) << "not init!";
        return;
    }
    globalSnapshotclient->UnInit();
}
