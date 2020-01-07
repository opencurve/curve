/*
 * Project: curve
 * Created Date: Tue Aug 06 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/common/curvefs_client.h"

using ::curve::client::SnapCloneClosure;

namespace curve {
namespace snapshotcloneserver {

class TestCurveFsClientImpl : public ::testing::Test {
 public:
    TestCurveFsClientImpl() {}

    virtual void SetUp() {
        client_ = std::make_shared<CurveFsClientImpl>();
        clientOption_.configPath = "test/snapshotcloneserver/client_test.conf";
        clientOption_.mdsRootUser = "root";
        clientOption_.mdsRootPassword = "1234";
        client_->Init(clientOption_);
    }

    virtual void TearDown() {
        client_->UnInit();
    }

 protected:
    std::shared_ptr<CurveFsClient> client_;
    CurveClientOptions clientOption_;
};

struct TestClosure : public SnapCloneClosure {
    void Run() {
        std::unique_ptr<TestClosure> selfGuard(this);
    }
};

TEST_F(TestCurveFsClientImpl, TestClientInterface) {
    uint64_t seq = 0;
    client_->CreateSnapshot("file1", "user1", &seq);
    client_->CreateSnapshot("file1", clientOption_.mdsRootUser, &seq);

    client_->DeleteSnapshot("file1", "user1", 1);
    client_->DeleteSnapshot("file1", clientOption_.mdsRootUser, 1);

    FInfo fInfo;
    client_->GetSnapshot("file1", "user1", 1, &fInfo);
    client_->GetSnapshot("file1", clientOption_.mdsRootUser, 1, &fInfo);

    SegmentInfo segInfo;
    client_->GetSnapshotSegmentInfo("file1", "user1", 1, 0, &segInfo);
    client_->GetSnapshotSegmentInfo(
        "file1", clientOption_.mdsRootUser, 1, 0, &segInfo);

    ChunkIDInfo cidinfo;
    client_->DeleteChunkSnapshotOrCorrectSn(cidinfo, 2);

    FileStatus fstatus;
    client_->CheckSnapShotStatus("file1", "user1", 1, &fstatus);
    client_->CheckSnapShotStatus(
        "file1", clientOption_.mdsRootUser, 1, &fstatus);

    ChunkInfoDetail chunkInfo;
    client_->GetChunkInfo(cidinfo, &chunkInfo);

    client_->CreateCloneFile("file1", "user1", 1024, 1, 1024, &fInfo);
    client_->CreateCloneFile(
        "file1", clientOption_.mdsRootUser, 1024, 1, 1024, &fInfo);

    TestClosure *cb = new TestClosure();
    client_->CreateCloneChunk("", cidinfo, 1, 2, 1024, cb);

    TestClosure *cb2 = new TestClosure();
    client_->RecoverChunk(cidinfo, 0, 1024, cb2);

    client_->CompleteCloneMeta("file1", "user1");
    client_->CompleteCloneMeta("file1", clientOption_.mdsRootUser);

    client_->CompleteCloneFile("file1", "user1");
    client_->CompleteCloneFile("file1", clientOption_.mdsRootUser);

    client_->GetFileInfo("file1", "user1", &fInfo);
    client_->GetFileInfo("file1", clientOption_.mdsRootUser, &fInfo);

    client_->GetOrAllocateSegmentInfo(true, 0, &fInfo, "user1", &segInfo);
    client_->GetOrAllocateSegmentInfo(
        true, 0, &fInfo, clientOption_.mdsRootUser, &segInfo);

    client_->RenameCloneFile("user1", 1, 2, "file1", "file2");
    client_->RenameCloneFile(clientOption_.mdsRootUser, 1, 2, "file1", "file2");

    client_->DeleteFile("file1", "user1", 1);
    client_->DeleteFile("file1", clientOption_.mdsRootUser, 1);

    client_->Mkdir("/clone", "user1");
    client_->Mkdir("/clone", clientOption_.mdsRootUser);

    client_->ChangeOwner("file1", "user2");
}



}  // namespace snapshotcloneserver
}  // namespace curve

