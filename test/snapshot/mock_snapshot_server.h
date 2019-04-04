/*
 * Project: curve
 * Created Date: Wed Dec 26 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_TEST_SNAPSHOT_MOCK_SNAPSHOT_SERVER_H_
#define CURVE_TEST_SNAPSHOT_MOCK_SNAPSHOT_SERVER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "src/snapshot/snapshot_core.h"
#include "src/snapshot/snapshot_service_manager.h"

namespace curve {
namespace snapshotserver {

class MockSnapshotCore : public SnapshotCore {
 public:
    MockSnapshotCore() {}
    virtual ~MockSnapshotCore() {}

    MOCK_METHOD4(CreateSnapshotPre,
        int(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo));

    MOCK_METHOD1(HandleCreateSnapshotTask,
        void(std::shared_ptr<SnapshotTaskInfo> task));

    MOCK_METHOD4(DeleteSnapshotPre,
        int(UUID uuid,
        const std::string &user,
        const std::string &fileName,
        SnapshotInfo *snapInfo));

    MOCK_METHOD1(HandleDeleteSnapshotTask,
        void(std::shared_ptr<SnapshotTaskInfo> task));

    MOCK_METHOD2(GetFileSnapshotInfo,
        int(const std::string &file,
        std::vector<SnapshotInfo> *info));

    MOCK_METHOD1(GetSnapshotList,
        int(std::vector<SnapshotInfo> *list));
};


class MockSnapshotMetaStore : public SnapshotMetaStore {
 public:
    MOCK_METHOD0(Init, int());
    MOCK_METHOD1(AddSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD1(DeleteSnapshot, int(UUID uuid));
    MOCK_METHOD1(UpdateSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD2(GetSnapshotInfo,
        int(UUID uuid, SnapshotInfo *info));
    MOCK_METHOD2(GetSnapshotList,
        int(const std::string &filename,
            std::vector<SnapshotInfo> *v));
    MOCK_METHOD1(GetSnapshotList,
        int(std::vector<SnapshotInfo> *list));
};

class MockSnapshotDataStore : public SnapshotDataStore {
 public:
    MOCK_METHOD0(Init, int());
    MOCK_METHOD2(PutChunkIndexData,
        int(const ChunkIndexDataName &name,
            const ChunkIndexData &meta));
    MOCK_METHOD2(GetChunkIndexData,
        int(const ChunkIndexDataName &name,
            ChunkIndexData *meta));
    MOCK_METHOD1(DeleteChunkIndexData,
        int(const ChunkIndexDataName &name));
    MOCK_METHOD1(ChunkIndexDataExist,
        bool(const ChunkIndexDataName &name));
    MOCK_METHOD2(GetChunkData,
        int(const ChunkDataName &name,
            ChunkData *data));
    MOCK_METHOD1(DeleteChunkData,
        int(const ChunkDataName &name));
    MOCK_METHOD1(ChunkDataExist,
        bool(const ChunkDataName &name));
    MOCK_METHOD2(SetSnapshotFlag,
        int(const ChunkIndexDataName &name, int flag));
    MOCK_METHOD1(GetSnapshotFlag,
        int(const ChunkIndexDataName &name));
    MOCK_METHOD2(DataChunkTranferInit,
        int(const ChunkDataName &name,
            std::shared_ptr<TransferTask> task));
    MOCK_METHOD5(DataChunkTranferAddPart,
        int(const ChunkDataName &name,
            std::shared_ptr<TransferTask> task,
            int partNum,
            int partSize,
            const char* buf));
    MOCK_METHOD2(DataChunkTranferComplete,
        int(const ChunkDataName &name,
            std::shared_ptr<TransferTask> task));
    MOCK_METHOD2(DataChunkTranferAbort,
        int(const ChunkDataName &name,
             std::shared_ptr<TransferTask> task));
};

class MockCurveFsClient : public CurveFsClient {
 public:
    MOCK_METHOD0(Init, int());
    MOCK_METHOD0(UnInit, int());
    MOCK_METHOD3(CreateSnapshot,
        int(const std::string &filename,
        const std::string &user,
        uint64_t *seq));
    MOCK_METHOD3(DeleteSnapshot,
        int(const std::string &filename,
            const std::string &user,
            uint64_t seq));
    MOCK_METHOD4(GetSnapshot,
        int(const std::string &filename,
            const std::string &user,
            uint64_t seq,
            FInfo *snapInfo));
    MOCK_METHOD5(GetSnapshotSegmentInfo,
        int(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo));
    MOCK_METHOD5(ReadChunkSnapshot,
        int(ChunkIDInfo cidinfo,
            uint64_t seq,
            uint64_t offset,
            uint64_t len,
            void *buf));
    MOCK_METHOD2(DeleteChunkSnapshot,
        int(ChunkIDInfo cidinfo,
        uint64_t seq));

    MOCK_METHOD3(CheckSnapShotStatus,
        int(std::string filename,
        std::string user,
        uint64_t seq));

    MOCK_METHOD2(GetChunkInfo,
        int(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo));
};

class MockSnapshotServiceManager : public SnapshotServiceManager {
 public:
    MockSnapshotServiceManager() :
     SnapshotServiceManager(nullptr, nullptr) {}
    ~MockSnapshotServiceManager() {}
    MOCK_METHOD4(CreateSnapshot,
        int(const std::string &file,
        const std::string &user,
        const std::string &desc,
        UUID *uuid));
    MOCK_METHOD3(DeleteSnapshot,
        int(UUID uuid,
        const std::string &user,
        const std::string &file));
    MOCK_METHOD3(GetFileSnapshotInfo,
        int(const std::string &file,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info));
    MOCK_METHOD3(CancelSnapshot,
        int(UUID uuid,
        const std::string &user,
        const std::string &file));
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_TEST_SNAPSHOT_MOCK_SNAPSHOT_SERVER_H_
