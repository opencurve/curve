/*
 * Project: curve
 * Created Date: Wed Dec 26 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_
#define TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>
#include <memory>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/snapshotcloneserver/common/config.h"

namespace curve {
namespace snapshotcloneserver {

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

    MOCK_METHOD2(GetSnapshotInfo,
        int(const UUID uuid, SnapshotInfo *info));
};

class MockSnapshotCloneMetaStore : public SnapshotCloneMetaStore {
 public:
    MOCK_METHOD1(Init, int(const SnapshotCloneMetaStoreOptions&));
    MOCK_METHOD1(AddSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD1(DeleteSnapshot, int(const UUID &uuid));
    MOCK_METHOD1(UpdateSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD2(GetSnapshotInfo,
        int(const UUID &uuid, SnapshotInfo *info));
    MOCK_METHOD2(GetSnapshotList,
        int(const std::string &filename,
            std::vector<SnapshotInfo> *v));
    MOCK_METHOD1(GetSnapshotList,
        int(std::vector<SnapshotInfo> *list));
    MOCK_METHOD1(AddCloneInfo, int(const CloneInfo &info));
    MOCK_METHOD1(DeleteCloneInfo, int(const std::string &taskID));
    MOCK_METHOD1(UpdateCloneInfo, int(const CloneInfo &info));
    MOCK_METHOD2(GetCloneInfo,
        int(const std::string &taskID, CloneInfo *info));
    MOCK_METHOD1(GetCloneInfoList,
        int(std::vector<CloneInfo> *list));
};

class MockSnapshotDataStore : public SnapshotDataStore {
 public:
    MOCK_METHOD1(Init, int(const std::string&));
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
    MOCK_METHOD1(Init, int(const CurveClientOptions &));
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
            char *buf));
    MOCK_METHOD2(DeleteChunkSnapshotOrCorrectSn,
        int(ChunkIDInfo cidinfo,
        uint64_t correctedSeq));

    MOCK_METHOD3(CheckSnapShotStatus,
        int(std::string filename,
        std::string user,
        uint64_t seq));

    MOCK_METHOD2(GetChunkInfo,
        int(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo));

    MOCK_METHOD6(CreateCloneFile,
        int(const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        FInfo* fileInfo));

    MOCK_METHOD5(CreateCloneChunk,
        int(const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize));

    MOCK_METHOD3(RecoverChunk,
        int(const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len));

    MOCK_METHOD2(CompleteCloneMeta,
        int(const std::string &filename,
        const std::string &user));

    MOCK_METHOD2(CompleteCloneFile,
        int(const std::string &filename,
        const std::string &user));

    MOCK_METHOD3(GetFileInfo,
        int(const std::string &filename,
        const std::string &user,
        FInfo* fileInfo));

    MOCK_METHOD5(GetOrAllocateSegmentInfo,
        int(bool allocate,
        uint64_t offset,
        const FInfo* fileInfo,
        const std::string user,
        SegmentInfo *segInfo));

    MOCK_METHOD5(RenameCloneFile,
            int(const std::string &user,
            uint64_t originId,
            uint64_t destinationId,
            const std::string &origin,
            const std::string &destination));

    MOCK_METHOD3(DeleteFile,
        int(const std::string &fileName,
        const std::string &user,
        uint64_t fileId));
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

class MockCloneServiceManager : public CloneServiceManager {
 public:
    MockCloneServiceManager() :
        CloneServiceManager(nullptr, nullptr) {}
    ~MockCloneServiceManager() {}

    MOCK_METHOD4(CloneFile,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag));

    MOCK_METHOD4(RecoverFile,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag));

    MOCK_METHOD2(GetCloneTaskInfo,
        int(const std::string &user,
        std::vector<TaskCloneInfo> *info));

    MOCK_METHOD2(CleanCloneTask,
        int(const std::string &user,
        const TaskIdType &taskId));
};

class MockCloneCore : public CloneCore {
 public:
    MOCK_METHOD6(CloneOrRecoverPre,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        CloneTaskType taskType,
        CloneInfo *info));

    MOCK_METHOD1(HandleCloneOrRecoverTask,
        void(std::shared_ptr<CloneTaskInfo> task));

    MOCK_METHOD3(CleanCloneOrRecoverTaskPre,
        int(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo));

    MOCK_METHOD1(HandleCleanCloneOrRecoverTask,
        void(std::shared_ptr<CloneTaskInfo> task));

    MOCK_METHOD1(GetCloneInfoList,
        int(std::vector<CloneInfo> *cloneInfos));

    MOCK_METHOD2(GetCloneInfo,
        int(TaskIdType taskId, CloneInfo *cloneInfo));

    MOCK_METHOD0(GetSnapshotRef,
        std::shared_ptr<SnapshotReference>());
};
}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_
