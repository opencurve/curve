/*************************************************************************
> File Name: fake_curvefs_client.cpp
> Author:
> Created Time: Thu 03 Jan 2019 10:14:14 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#include "test/snapshot/fake_curvefs_client.h"
#include <atomic>
#include <glog/logging.h> //NOLINT
#include <cstring>
static uint64_t global_seq = 0;
const uint32_t CHUNKSIZE = 16777216;
const uint32_t SEGMENTSIZE = 1073741824;
const uint64_t LENGTH = 5368709120;

namespace curve {
namespace snapshotserver {
int FakeCurveClient::Init() {
    LOG(INFO) << "FakeCurveClient Init";
    return 0;
}
int FakeCurveClient::UnInit() {
    LOG(INFO) << "FakeCurveClient UnInit";
    return 0;
}
int FakeCurveClient::CreateSnapshot(const std::string &filename,
    uint64_t *seq) {
    // increment seq
    global_seq += 1;
    *seq = global_seq;
    // add to fileMap_
    SnapFileInfo info;
    {
        info.finfo.id = 1;
        filename.copy(info.finfo.filename, sizeof(info.finfo.filename));
        info.finfo.parentid = 1;
        info.finfo.filetype = FileType::INODE_PAGEFILE;
        info.finfo.chunksize = CHUNKSIZE;
        info.finfo.segmentsize = SEGMENTSIZE;
        info.finfo.length = LENGTH;
        info.finfo.ctime = 99999;
        info.finfo.seqnum = global_seq;
    }
    fileMap_.emplace(filename, info);
    return 0;
}
int FakeCurveClient::DeleteSnapshot(const std::string &filename, uint64_t seq) {
    // del from fileMap_
    auto search = fileMap_.find(filename);
    if (search == fileMap_.end()) {
        LOG(ERROR) << "Can not find snapshot";
        return -1;
    }
    fileMap_.erase(search);
    return 0;
}
int FakeCurveClient::ListSnapshot(const std::string &filename,
    uint64_t seq,
    SnapFileInfo *snapInfo) {
    // search map
    auto search = fileMap_.find(filename);
    if (search == fileMap_.end()) {
        LOG(ERROR) << "Can not find snapshot";
        return -1;
    }
    *snapInfo = search->second;
    return 0;
}
int FakeCurveClient::GetSnapshotSegmentInfo(const std::string &filename,
    uint64_t seq,
    uint64_t offset,
    SegmentInfo *segInfo) {
    // search map
    // return SnapFileInfo.SegmentInfo
    segInfo->segmentsize = SEGMENTSIZE;
    segInfo->chunksize = CHUNKSIZE;
    segInfo->startoffset = 0;
    for (int i=0; i < 16; i++) {
        segInfo->chunkvec.push_back(chunkidinfo(i, i, i));
    }
    return 0;
}
int FakeCurveClient::ReadChunkSnapshot(
    LogicPoolID lpid,
    CopysetID cpid,
    ChunkID chunkID,
    uint64_t seq,
    uint64_t offset,
    uint64_t len,
    void *buf) {
    // set the buf
    char *src = new char[len]{0};
    std::memcpy(buf, src, len);
    delete [] src;
    return len;
}
int FakeCurveClient::DeleteChunkSnapshot(LogicPoolID lpid,
        CopysetID cpid,
        ChunkID chunkId,
        uint64_t seq) {
    // delete chunk
    return 0;
}
int FakeCurveClient::GetChunkInfo(
        LogicPoolID lpid,
        CopysetID cpid,
        ChunkID chunkId,
        ChunkInfoDetail *chunkInfo) {
    chunkInfo->chunkSn.push_back(1);
    return 0;
}
}  // namespace snapshotserver
}  // namespace curve
