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
 * Created Date: Wed Dec 26 2018
 * Author: xuchaojie
 */

#ifndef TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_
#define TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>
#include <memory>
#include <map>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/snapshotcloneserver/volume/volume_service_manager.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/kvstorageclient/etcd_client.h"

using ::curve::kvstorage::KVStorageClient;

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

    MOCK_METHOD4(CreateLocalSnapshot,
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

    MOCK_METHOD3(DeleteLocalSnapshot,
        int(UUID uuid,
        const std::string &user,
        const std::string &fileName));

    MOCK_METHOD1(HandleDeleteSnapshotTask,
        void(std::shared_ptr<SnapshotTaskInfo> task));

    MOCK_METHOD3(GetFileInfo,
        int(const std::string &file,
        const std::string &user,
        FInfo *fInfo));

    MOCK_METHOD3(GetFileSnapshotInfo,
        int(const std::string &file,
        const std::string &user,
        std::vector<SnapshotInfo> *info));

    MOCK_METHOD5(GetLocalSnapshotStatus,
        int(const std::string &file,
        const std::string &user,
        uint64_t seq,
        Status *status,
        uint32_t *progress));

    MOCK_METHOD1(GetSnapshotList,
        int(std::vector<SnapshotInfo> *list));

    MOCK_METHOD2(GetSnapshotInfo,
        int(const UUID uuid, SnapshotInfo *info));

    MOCK_METHOD3(GetSnapshotInfo,
        int(const std::string &file,
        const std::string &snapshotName,
        SnapshotInfo *info));

    MOCK_METHOD1(HandleCancelUnSchduledSnapshotTask,
        int(std::shared_ptr<SnapshotTaskInfo> task));

    MOCK_METHOD1(HandleCancelScheduledSnapshotTask,
                 int(std::shared_ptr<SnapshotTaskInfo> task));
};

class MockSnapshotCloneMetaStore : public SnapshotCloneMetaStore {
 public:
    MOCK_METHOD1(AddSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD1(DeleteSnapshot, int(const UUID &uuid));
    MOCK_METHOD1(UpdateSnapshot, int(const SnapshotInfo &snapinfo));
    MOCK_METHOD2(CASSnapshot, int(const UUID&, CASFunc));
    MOCK_METHOD2(GetSnapshotInfo,
        int(const UUID &uuid, SnapshotInfo *info));
    MOCK_METHOD3(GetSnapshotInfo,
        int(const std::string &file,
        const std::string &snapshotName,
        SnapshotInfo *info));
    MOCK_METHOD2(GetSnapshotList,
        int(const std::string &filename,
            std::vector<SnapshotInfo> *v));
    MOCK_METHOD1(GetSnapshotList,
        int(std::vector<SnapshotInfo> *list));
    MOCK_METHOD0(GetSnapshotCount,
        uint32_t());
    MOCK_METHOD1(AddCloneInfo, int(const CloneInfo &info));
    MOCK_METHOD1(DeleteCloneInfo, int(const std::string &taskID));
    MOCK_METHOD1(UpdateCloneInfo, int(const CloneInfo &info));
    MOCK_METHOD2(GetCloneInfo,
        int(const std::string &taskID, CloneInfo *info));
    MOCK_METHOD2(GetCloneInfoByFileName,
        int(const std::string &fileName, std::vector<CloneInfo> *list));
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

    MOCK_METHOD6(CreateFile,
        int(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset));

    MOCK_METHOD2(DeleteFile,
        int(const std::string &file,
        const std::string &user));

    MOCK_METHOD3(ListDir,
        int(const std::string &dir,
        const std::string &user,
        std::vector<FInfo_t> *finfoVec));

    MOCK_METHOD3(CreateSnapshot,
        int(const std::string &filename,
        const std::string &user,
        FInfo* snapInfo));

    MOCK_METHOD3(DeleteSnapshot,
        int(const std::string &filename,
            const std::string &user,
            uint64_t seq));

    MOCK_METHOD3(ListSnapshot,
        int(const std::string &filename,
        const std::string &user,
        std::map<uint64_t, FInfo> *snapif));

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
    MOCK_METHOD6(ReadChunkSnapshot,
        int(ChunkIDInfo cidinfo,
            uint64_t seq,
            uint64_t offset,
            uint64_t len,
            char *buf,
            SnapCloneClosure* scc));
    MOCK_METHOD2(DeleteChunkSnapshotOrCorrectSn,
        int(const ChunkIDInfo &cidinfo,
        uint64_t correctedSeq));

    MOCK_METHOD5(CheckSnapShotStatus,
        int(std::string filename,
        std::string user,
        uint64_t seq,
        FileStatus* filestatus,
        uint32_t* progress));

    MOCK_METHOD2(GetChunkInfo,
        int(const ChunkIDInfo &cidinfo,
        ChunkInfoDetail *chunkInfo));

    MOCK_METHOD10(CreateCloneFile,
        int(const std::string &source,
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string& poolset,
        FInfo* fileInfo));

    MOCK_METHOD6(CreateCloneChunk,
        int(const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize,
        SnapCloneClosure* scc));

    MOCK_METHOD4(RecoverChunk,
        int(const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure* scc));

    MOCK_METHOD2(CompleteCloneMeta,
        int(const std::string &filename,
        const std::string &user));

    MOCK_METHOD2(CompleteCloneFile,
        int(const std::string &filename,
        const std::string &user));

    MOCK_METHOD3(SetCloneFileStatus,
        int(const std::string &filename,
        const FileStatus& filestatus,
        const std::string &user));

    MOCK_METHOD3(GetFileInfo,
        int(const std::string &filename,
        const std::string &user,
        FInfo* fileInfo));

    MOCK_METHOD5(GetOrAllocateSegmentInfo,
        int(bool allocate,
        uint64_t offset,
        FInfo* fileInfo,
        const std::string &user,
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

    MOCK_METHOD2(Mkdir,
        int(const std::string& dirpath,
        const std::string &user));

    MOCK_METHOD2(ChangeOwner,
        int(const std::string& filename,
            const std::string& newOwner));

    MOCK_METHOD6(Clone,
        int(const std::string &snapPath,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        bool readonlyFlag,
        FInfo* finfo));

    MOCK_METHOD2(Flatten,
        int(const std::string &file,
        const std::string &user));

    MOCK_METHOD4(QueryFlattenStatus,
        int(const std::string &file,
        const std::string &user,
        FileStatus* filestatus,
        uint32_t* progress));

    MOCK_METHOD2(ProtectSnapshot,
        int(const std::string &snapPath,
        const std::string user));

    MOCK_METHOD2(UnprotectSnapshot,
        int(const std::string &snapPath,
        const std::string user));
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

    MOCK_METHOD4(CreateS3Snapshot,
        int(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        UUID *uuid));

    MOCK_METHOD4(CreateLocalSnapshot,
        int(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        UUID *uuid));

    MOCK_METHOD3(DeleteSnapshotBySnapshotName,
        int(const std::string &snapshotName,
        const std::string &user,
        const std::string &file));

    MOCK_METHOD3(DeleteSnapshot,
        int(const UUID &uuid,
        const std::string &user,
        const std::string &file));

    MOCK_METHOD3(DeleteS3Snapshot,
        int(const UUID &uuid,
        const std::string &user,
        const std::string &file));

    MOCK_METHOD3(DeleteLocalSnapshot,
        int(const UUID &uuid,
        const std::string &user,
        const std::string &file));

    MOCK_METHOD3(GetFileSnapshotInfo,
        int(const std::string &file,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info));

    MOCK_METHOD4(GetFileSnapshotInfoById,
        int(const std::string &file,
        const std::string &user,
        const UUID &uuid,
        std::vector<FileSnapshotInfo> *info));

    MOCK_METHOD4(GetFileSnapshotInfoBySnapshotName,
        int(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        std::vector<FileSnapshotInfo> *info));

    MOCK_METHOD2(GetSnapshotListByFilter,
        int(const SnapshotFilterCondition &filter,
        std::vector<FileSnapshotInfo> *info));

    MOCK_METHOD3(CancelSnapshot,
        int(const UUID &uuid,
        const std::string &user,
        const std::string &file));
};

class MockCloneServiceManager : public CloneServiceManager {
 public:
    MockCloneServiceManager() :
        CloneServiceManager(nullptr, nullptr, nullptr) {}
    ~MockCloneServiceManager() {}

    MOCK_METHOD7(CloneFile,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> entity,
        TaskIdType *taskId));

    MOCK_METHOD6(RecoverFile,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> entity,
        TaskIdType *taskId));

    MOCK_METHOD2(Flatten,
        int(const std::string &user,
        const TaskIdType &taskId));

    MOCK_METHOD2(GetCloneTaskInfo,
        int(const std::string &user,
        std::vector<TaskCloneInfo> *info));

    MOCK_METHOD1(GetCloneTaskInfo,
        int(std::vector<TaskCloneInfo> *info));

    MOCK_METHOD3(GetCloneTaskInfoById,
        int(const std::string &user,
        const TaskIdType &taskId,
        std::vector<TaskCloneInfo> *info));

    MOCK_METHOD3(GetCloneTaskInfoByName,
        int(const std::string &user,
        const std::string &fileName,
        std::vector<TaskCloneInfo> *info));

    MOCK_METHOD2(GetCloneTaskInfoByFilter,
        int(const CloneFilterCondition &filter,
        std::vector<TaskCloneInfo> *info));

    MOCK_METHOD2(CleanCloneTask,
        int(const std::string &user,
        const TaskIdType &taskId));

    MOCK_METHOD3(GetCloneRefStatus,
        int(const std::string &src,
        CloneRefStatus *refStatus,
        std::vector<CloneInfo> *needCheckFiles));
};

class MockVolumeServiceManager : public VolumeServiceManager {
 public:
    MockVolumeServiceManager() :
        VolumeServiceManager(nullptr) {}
    ~MockVolumeServiceManager() {}

    MOCK_METHOD6(CreateFile,
        int(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset));

    MOCK_METHOD2(DeleteFile,
        int(const std::string &file,
        const std::string &user));

    MOCK_METHOD3(GetFile,
        int(const std::string &file,
        const std::string &user,
        FileInfo *fileInfo));

    MOCK_METHOD3(ListFile,
        int(const std::string &dir,
        const std::string &user,
        std::vector<FileInfo> *fileInfos));
};

class MockCloneCore : public CloneCore {
 public:
    MOCK_METHOD6(CloneLocal, int(const std::string &file,
        const std::string &snapshotName,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        bool readonlyFlag));

    MOCK_METHOD2(FlattenLocal, int(const std::string &file,
        const std::string &user));

    MOCK_METHOD7(CloneOrRecoverPre,
        int(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        CloneTaskType taskType,
        std::string poolset,
        CloneInfo *info));

    MOCK_METHOD1(HandleCloneOrRecoverTask,
        void(std::shared_ptr<CloneTaskInfo> task));

    MOCK_METHOD3(CleanCloneOrRecoverTaskPre,
        int(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo));

    MOCK_METHOD1(HandleCleanCloneOrRecoverTask,
        void(std::shared_ptr<CloneTaskInfo> task));

    MOCK_METHOD3(FlattenPre,
        int(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo));

    MOCK_METHOD1(GetCloneInfoList,
        int(std::vector<CloneInfo> *cloneInfos));

    MOCK_METHOD2(GetCloneInfo,
        int(TaskIdType taskId, CloneInfo *cloneInfo));

    MOCK_METHOD2(GetCloneInfoByFileName,
        int(const std::string &fileName, std::vector<CloneInfo> *list));

    MOCK_METHOD0(GetSnapshotRef,
        std::shared_ptr<SnapshotReference>());

    MOCK_METHOD0(GetCloneRef,
        std::shared_ptr<CloneReference>());

    MOCK_METHOD1(HandleRemoveCloneOrRecoverTask,
        int(std::shared_ptr<CloneTaskInfo> task));

    MOCK_METHOD2(CheckFileExists,
        int(const std::string &filename, uint64_t inodeId));

    MOCK_METHOD1(HandleDeleteCloneInfo,
        int(const CloneInfo &cloneInfo));
};

class MockCloneServiceManagerBackend : public CloneServiceManagerBackend {
 public:
    MOCK_METHOD0(Func, void());
    MOCK_METHOD2(Init, void(uint32_t recordIntevalMs, uint32_t roundIntevalMs));
    MOCK_METHOD0(Start, void());
    MOCK_METHOD0(Stop, void());
};

class MockKVStorageClient : public KVStorageClient {
 public:
    virtual ~MockKVStorageClient() {}
    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List,
        int(const std::string&, const std::string&, std::vector<std::string>*));
    MOCK_METHOD1(Delete, int(const std::string&));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation>&));
    MOCK_METHOD3(CompareAndSwap, int(const std::string&, const std::string&,
        const std::string&));
    MOCK_METHOD5(CampaignLeader, int(const std::string&, const std::string&,
        uint32_t, uint32_t, uint64_t*));
    MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string&));
    MOCK_METHOD2(LeaderKeyExist, bool(uint64_t, uint64_t));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t *));
    MOCK_METHOD6(ListWithLimitAndRevision,
        int(const std::string&, const std::string&,
        int64_t, int64_t, std::vector<std::string>*, std::string *));
    MOCK_METHOD3(PutRewithRevision, int(const std::string &,
        const std::string &, int64_t *));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string &, int64_t *));
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_SNAPSHOTCLONESERVER_MOCK_SNAPSHOT_SERVER_H_
