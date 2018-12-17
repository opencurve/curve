/*************************************************************************
> File Name: fake_curvefs_client.h
> Author:
> Created Time: Thu 03 Jan 2019 09:18:08 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef _FAKE_CURVEFS_CLIENT_H
#define _FAKE_CURVEFS_CLIENT_H
#include <string>
#include <memory>
#include <map>
#include <vector>

#include "src/snapshot/curvefs_client.h"
namespace curve {
namespace snapshotserver {
class FakeCurveClient : public CurveFsClient {
 public:
    FakeCurveClient() {}
    ~FakeCurveClient() {}

    int Init() override;
    int UnInit() override;
    int CreateSnapshot(const std::string &filename,
         uint64_t *seq) override;
    int DeleteSnapshot(const std::string &filename, uint64_t seq) override;
    int ListSnapshot(const std::string &filename,
         uint64_t seq,
         SnapFileInfo *snapInfo) override;
    int GetSnapshotSegmentInfo(const std::string &filename,
         uint64_t seq,
         uint64_t offset,
         SegmentInfo *segInfo) override;
    int ReadChunkSnapshot(
        LogicPoolID lpid,
        CopysetID cpid,
        ChunkID chunkID,
        uint64_t seq,
        uint64_t offset,
        uint64_t len,
        void *buf) override;
    int DeleteChunkSnapshot(LogicPoolID lpid,
        CopysetID cpid,
        ChunkID chunkId,
        uint64_t seq) override;
    int GetChunkInfo(
        LogicPoolID lpid,
        CopysetID cpid,
        ChunkID chunkId,
        ChunkInfoDetail *chunkInfo) override;

 private:
    std::map<std::string, SnapFileInfo> fileMap_;
};
}  // namespace snapshotserver
}  // namespace curve
#endif
