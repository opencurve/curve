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
        std::shared_ptr<SnapshotClient> snapClient =
            std::make_shared<SnapshotClient>();
        std::shared_ptr<FileClient> fileClient =
            std::make_shared<FileClient>();
        client_ = std::make_shared<CurveFsClientImpl>(snapClient, fileClient);
        clientOption_.configPath = "test/snapshotcloneserver/client_test.conf";
        clientOption_.mdsRootUser = "root";
        clientOption_.mdsRootPassword = "1234";
        clientOption_.clientMethodRetryTimeSec = 1;
        clientOption_.clientMethodRetryIntervalMs = 500;
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

TEST_F(TestCurveFsClientImpl, TestClientInterfaceFail) {
    uint64_t seq = 0;
    int ret = client_->CreateSnapshot("file1", "user1", &seq);
    ASSERT_LT(ret, 0);
    ret = client_->CreateSnapshot("file1", clientOption_.mdsRootUser, &seq);
    ASSERT_LT(ret, 0);

    ret = client_->DeleteSnapshot("file1", "user1", 1);
    ASSERT_LT(ret, 0);
    ret = client_->DeleteSnapshot("file1", clientOption_.mdsRootUser, 1);
    ASSERT_LT(ret, 0);

    FInfo fInfo;
    ret = client_->GetSnapshot("file1", "user1", 1, &fInfo);
    ASSERT_LT(ret, 0);
    ret = client_->GetSnapshot("file1", clientOption_.mdsRootUser, 1, &fInfo);
    ASSERT_LT(ret, 0);

    SegmentInfo segInfo;
    ret = client_->GetSnapshotSegmentInfo("file1", "user1", 1, 0, &segInfo);
    ASSERT_LT(ret, 0);
    ret = client_->GetSnapshotSegmentInfo(
        "file1", clientOption_.mdsRootUser, 1, 0, &segInfo);
    ASSERT_LT(ret, 0);

    ChunkIDInfo cidinfo;
    FileStatus fstatus;
    ret = client_->CheckSnapShotStatus("file1", "user1", 1, &fstatus);
    ASSERT_LT(ret, 0);
    ret = client_->CheckSnapShotStatus(
        "file1", clientOption_.mdsRootUser, 1, &fstatus);
    ASSERT_LT(ret, 0);

    ChunkInfoDetail chunkInfo;
    ret = client_->GetChunkInfo(cidinfo, &chunkInfo);
    ASSERT_LT(ret, 0);

    ret = client_->CreateCloneFile("file1", "user1", 1024, 1, 1024, &fInfo);
    ASSERT_LT(ret, 0);
    ret = client_->CreateCloneFile(
        "file1", clientOption_.mdsRootUser, 1024, 1, 1024, &fInfo);
    ASSERT_LT(ret, 0);

    TestClosure *cb = new TestClosure();
    ret = client_->CreateCloneChunk("", cidinfo, 1, 2, 1024, cb);
    ASSERT_EQ(ret, 0);

    TestClosure *cb2 = new TestClosure();
    ret = client_->RecoverChunk(cidinfo, 0, 1024, cb2);
    ASSERT_EQ(ret, 0);

    ret = client_->CompleteCloneMeta("file1", "user1");
    ASSERT_LT(ret, 0);
    ret = client_->CompleteCloneMeta("file1", clientOption_.mdsRootUser);
    ASSERT_LT(ret, 0);

    ret = client_->CompleteCloneFile("file1", "user1");
    ASSERT_LT(ret, 0);
    ret = client_->CompleteCloneFile("file1", clientOption_.mdsRootUser);
    ASSERT_LT(ret, 0);

    ret = client_->GetFileInfo("file1", "user1", &fInfo);
    ASSERT_LT(ret, 0);

    ret = client_->GetFileInfo("file1", clientOption_.mdsRootUser, &fInfo);
    ASSERT_LT(ret, 0);

    //  client 对mds接口无限重试，这两个接口死循环，先注释掉
    // ret = client_->GetOrAllocateSegmentInfo(
    //     true, 0, &fInfo, "user1", &segInfo);
    // ASSERT_LT(ret, 0);
    // ret = client_->GetOrAllocateSegmentInfo(
    //     true, 0, &fInfo, clientOption_.mdsRootUser, &segInfo);
    // ASSERT_LT(ret, 0);

    ret = client_->RenameCloneFile("user1", 1, 2, "file1", "file2");
    ASSERT_LT(ret, 0);
    ret = client_->RenameCloneFile(
        clientOption_.mdsRootUser, 1, 2, "file1", "file2");
    ASSERT_LT(ret, 0);

    ret = client_->DeleteFile("file1", "user1", 1);
    ASSERT_LT(ret, 0);
    ret = client_->DeleteFile("file1", clientOption_.mdsRootUser, 1);
    ASSERT_LT(ret, 0);

    ret = client_->Mkdir("/clone", "user1");
    ASSERT_LT(ret, 0);
    ret = client_->Mkdir("/clone", clientOption_.mdsRootUser);
    ASSERT_LT(ret, 0);

    ret = client_->ChangeOwner("file1", "user2");
    ASSERT_LT(ret, 0);
}



}  // namespace snapshotcloneserver
}  // namespace curve

