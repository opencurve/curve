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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <vector>
#include <memory>
#include <map>
#include <string>

#include "src/snapshotcloneserver/common/snapshotclone_meta_store_etcd.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/snapshotcloneserver/common/snapshotclonecodec.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "test/snapshotcloneserver/mock_snapshot_server.h"

using ::curve::kvstorage::StorageClient;
using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Matcher;

namespace curve {
namespace snapshotcloneserver {

static const char* kDefaultPoolset = "poolset";

class TestSnapshotCloneMetaStoreEtcd : public ::testing::Test {
 public:
    TestSnapshotCloneMetaStoreEtcd() {}
    virtual ~TestSnapshotCloneMetaStoreEtcd() {}

    void SetUp() {
        kvStorageClient_ = std::make_shared<MockStorageClient>();
        codec_ = std::make_shared<SnapshotCloneCodec>();
        metaStore_ = std::make_shared<SnapshotCloneMetaStoreEtcd>(
            kvStorageClient_, codec_);
    }

    void TearDown() {
        metaStore_ = nullptr;
    }

 protected:
    std::shared_ptr<MockStorageClient> kvStorageClient_;
    std::shared_ptr<SnapshotCloneCodec> codec_;
    std::shared_ptr<SnapshotCloneMetaStoreEtcd> metaStore_;
};

bool JudgeCloneInfoEqual(const CloneInfo &left, const CloneInfo &right) {
    if (left.GetTaskId() == right.GetTaskId() &&
        left.GetUser() == right.GetUser() &&
        left.GetTaskType() == right.GetTaskType() &&
        left.GetSrc() == right.GetSrc() &&
        left.GetDest() == right.GetDest() &&
        left.GetOriginId() == right.GetOriginId() &&
        left.GetDestId() == right.GetDestId() &&
        left.GetTime() == right.GetTime() &&
        left.GetFileType() == right.GetFileType() &&
        left.GetIsLazy() == right.GetIsLazy() &&
        left.GetNextStep() == right.GetNextStep() &&
        left.GetStatus() == right.GetStatus()) {
        return true;
    }
    return false;
}

bool JudgeSnapshotInfoEqual(const SnapshotInfo &left,
    const SnapshotInfo &right) {
    if (left.GetUuid() == right.GetUuid() &&
        left.GetUser() == right.GetUser() &&
        left.GetFileName() == right.GetFileName() &&
        left.GetSnapshotName() == right.GetSnapshotName() &&
        left.GetSeqNum() == right.GetSeqNum() &&
        left.GetChunkSize() == right.GetChunkSize() &&
        left.GetSegmentSize() == right.GetSegmentSize() &&
        left.GetFileLength() == right.GetFileLength() &&
        left.GetStripeUnit() == right.GetStripeUnit() &&
        left.GetStripeCount() == right.GetStripeCount() &&
        left.GetPoolset() == right.GetPoolset() &&
        left.GetCreateTime() == right.GetCreateTime() &&
        left.GetStatus() == right.GetStatus()) {
        return true;
    }
    return false;
}

// snapInfo

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestAddSnapInfoAndGetSuccess) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    SnapshotInfo outInfo;
    ret = metaStore_->GetSnapshotInfo("snapuuid", &outInfo);
    ASSERT_EQ(0, ret);

    ASSERT_TRUE(JudgeSnapshotInfoEqual(snapInfo, outInfo));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestAddSnapshotInfoPutInfoEtcdFail) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestDeleteSnapshotSuccess) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);


    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    ret = metaStore_->DeleteSnapshot("snapuuid");
    ASSERT_EQ(0, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestDeleteSnapshotDeleteFromEtcdFail) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);


    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    ret = metaStore_->DeleteSnapshot("snapuuid");
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateSnapshotAndGetSuccess) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    SnapshotInfo snapInfo2("snapuuid", "snapuser2", "file2", "snapxxx2", 101,
                        1025, 2049, 4097, 1, 0, kDefaultPoolset, 0,
                        Status::done);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    ret = metaStore_->UpdateSnapshot(snapInfo2);
    ASSERT_EQ(0, ret);

    SnapshotInfo outInfo;
    ret = metaStore_->GetSnapshotInfo("snapuuid", &outInfo);
    ASSERT_EQ(0, ret);

    ASSERT_TRUE(JudgeSnapshotInfoEqual(snapInfo2, outInfo));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateSnapshotPutInfoEtcdFail) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    SnapshotInfo snapInfo2("snapuuid", "snapuser2", "file2", "snapxxx2", 101,
                        1025, 2049, 4097, 0, 0, kDefaultPoolset, 1,
                        Status::done);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    ret = metaStore_->UpdateSnapshot(snapInfo2);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateSnapshotNotExistAndGetSuccess) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->UpdateSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    SnapshotInfo outInfo;
    ret = metaStore_->GetSnapshotInfo("snapuuid", &outInfo);
    ASSERT_EQ(0, ret);

    ASSERT_TRUE(JudgeSnapshotInfoEqual(snapInfo, outInfo));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd, TestCASSnapshot) {
    SnapshotInfo snapInfo(
        "uuid", "user", "", "", 0, 0, 0, 0, 0, 0, kDefaultPoolset,
        0, Status::pending);

    auto setUp = [&]() {
        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));
        ASSERT_EQ(metaStore_->AddSnapshot(snapInfo), 0);
    };

    auto tearDown = [&]() {
        ASSERT_EQ(metaStore_->DeleteSnapshot("uuid"), 0);
    };

    // CASE 1: Etcd put failed -> CASSnapshot failed
    {
        setUp();

        auto cas = [](SnapshotInfo* snapinfo) -> SnapshotInfo* {
            return snapinfo;
        };

        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdUnknown));
        ASSERT_EQ(metaStore_->CASSnapshot("uuid", cas), -1);

        tearDown();
    }

    // CASE 2: Set snapshot success
    {
        setUp();

        SnapshotInfo setInfo(
            "uuid", "user1", "", "", 1, 1, 1, 1, 1, 1, kDefaultPoolset,
            1, Status::done);
        auto cas = [&setInfo](SnapshotInfo* snapinfo) -> SnapshotInfo* {
            return &setInfo;
        };

        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));
        ASSERT_EQ(metaStore_->CASSnapshot("uuid", cas), 0);

        SnapshotInfo retInfo;
        ASSERT_EQ(metaStore_->GetSnapshotInfo("uuid", &retInfo), 0);
        ASSERT_TRUE(JudgeSnapshotInfoEqual(retInfo, setInfo));

        tearDown();
    }

    // CASE 3: Set snapshot success without origin snapshot
    {
        auto cas = [&snapInfo](SnapshotInfo* snapinfo) -> SnapshotInfo* {
            return &snapInfo;
        };

        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));
        ASSERT_EQ(metaStore_->CASSnapshot("uuid", cas), 0);

        SnapshotInfo retInfo;
        ASSERT_EQ(metaStore_->GetSnapshotInfo("uuid", &retInfo), 0);
        ASSERT_TRUE(JudgeSnapshotInfoEqual(retInfo, snapInfo));

        tearDown();
    }

    // CASE 4: Not needed to set snapshot
    {
        setUp();

        auto cas = [](SnapshotInfo* snapinfo) -> SnapshotInfo* {
            return nullptr;
        };

        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .Times(0);
        ASSERT_EQ(metaStore_->CASSnapshot("uuid", cas), 0);

        SnapshotInfo retInfo;
        ASSERT_EQ(metaStore_->GetSnapshotInfo("uuid", &retInfo), 0);
        ASSERT_TRUE(JudgeSnapshotInfoEqual(retInfo, snapInfo));

        tearDown();
    }

    // CASE 5: Set snapshot and keep status
    {
        setUp();

        SnapshotInfo setInfo(
            "uuid", "user1", "", "", 1, 1, 1, 1, 1, 1, kDefaultPoolset,
            1, Status::done);
        auto cas = [&setInfo](SnapshotInfo* snapInfo) -> SnapshotInfo* {
            setInfo.SetStatus(snapInfo->GetStatus());
            return &setInfo;
        };

        EXPECT_CALL(*kvStorageClient_, Put(_, _))
            .WillOnce(Return(EtcdErrCode::EtcdOK));
        ASSERT_EQ(metaStore_->CASSnapshot("uuid", cas), 0);

        SnapshotInfo retInfo;
        ASSERT_EQ(metaStore_->GetSnapshotInfo("uuid", &retInfo), 0);
        ASSERT_TRUE(JudgeSnapshotInfoEqual(retInfo, setInfo));
        ASSERT_EQ(retInfo.GetStatus(), Status::pending);

        tearDown();
    }
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetSnapshotInfoFail) {
    SnapshotInfo outInfo;
    int ret = metaStore_->GetSnapshotInfo("snapuuid", &outInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetSnapshotList1Success) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    std::vector<SnapshotInfo> list;
    ret = metaStore_->GetSnapshotList("file1", &list);
    ASSERT_EQ(0, ret);

    ASSERT_EQ(1, list.size());
    ASSERT_TRUE(JudgeSnapshotInfoEqual(snapInfo, list[0]));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetSnapshotList1Fail) {
    std::vector<SnapshotInfo> list;
    int ret = metaStore_->GetSnapshotList("file1", &list);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetSnapshotList2Success) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddSnapshot(snapInfo);
    ASSERT_EQ(0, ret);

    std::vector<SnapshotInfo> list;
    ret = metaStore_->GetSnapshotList(&list);
    ASSERT_EQ(0, ret);

    ASSERT_EQ(1, list.size());
    ASSERT_TRUE(JudgeSnapshotInfoEqual(snapInfo, list[0]));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetSnapshotList2Fail) {
    std::vector<SnapshotInfo> list;
    int ret = metaStore_->GetSnapshotList(&list);
    ASSERT_EQ(-1, ret);
}

// cloneInfo

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestAddCloneInfoAndGetSuccess) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    CloneInfo outInfo;
    ret = metaStore_->GetCloneInfo("uuid1", &outInfo);
    ASSERT_EQ(0, ret);

    ASSERT_TRUE(JudgeCloneInfoEqual(cloneInfo, outInfo));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestAddCloneInfoPutInfoEtcdFail) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestDeleteCloneInfoSuccess) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    ret = metaStore_->DeleteCloneInfo("uuid1");
    ASSERT_EQ(0, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestDeleteCloneInfoDeleteFromEtcdFail) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    ret = metaStore_->DeleteCloneInfo("uuid1");
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateCloneInfoAndGetSuccess) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset,  1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    CloneInfo cloneInfo2("uuid1", "user2",
                     CloneTaskType::kClone, "src2",
                     "dst2", kDefaultPoolset, 2, 3, 4,
                     CloneFileType::kFile, false,
                     CloneStep::kEnd,
                     CloneStatus::done);

    ret = metaStore_->UpdateCloneInfo(cloneInfo2);
    ASSERT_EQ(0, ret);

    CloneInfo outInfo;
    ret = metaStore_->GetCloneInfo("uuid1", &outInfo);
    ASSERT_EQ(0, ret);

    ASSERT_TRUE(JudgeCloneInfoEqual(cloneInfo2, outInfo));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateCloneInfoPutInfoEtcdFail) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset,  1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    ret = metaStore_->UpdateCloneInfo(cloneInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestUpdateCloneInfoNotExist) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset,  1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));

    int ret = metaStore_->UpdateCloneInfo(cloneInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetCloneInfoFail) {
    CloneInfo outInfo;
    int ret = metaStore_->GetCloneInfo("uuid1", &outInfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetCloneInfoByFileNameSuccess) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset,  1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    std::vector<CloneInfo> list;
    ret = metaStore_->GetCloneInfoByFileName("dst1", &list);
    ASSERT_EQ(0, ret);

    ASSERT_EQ(1, list.size());
    ASSERT_TRUE(JudgeCloneInfoEqual(cloneInfo, list[0]));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetCloneInfoByFileNameFail) {
    std::vector<CloneInfo> list;
    int ret = metaStore_->GetCloneInfoByFileName("dst1", &list);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetCloneInfoListSuccess) {
    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset,  1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    int ret = metaStore_->AddCloneInfo(cloneInfo);
    ASSERT_EQ(0, ret);

    std::vector<CloneInfo> list;
    ret = metaStore_->GetCloneInfoList(&list);
    ASSERT_EQ(0, ret);

    ASSERT_EQ(1, list.size());
    ASSERT_TRUE(JudgeCloneInfoEqual(cloneInfo, list[0]));
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestGetCloneInfoListFail) {
    std::vector<CloneInfo> list;
    int ret = metaStore_->GetCloneInfoList(&list);
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestInitSuccess) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, kDefaultPoolset, 0,
                        Status::pending);
    SnapshotCloneCodec codec;
    std::string value;
    ASSERT_TRUE(codec.EncodeSnapshotData(snapInfo, &value));
    std::vector<std::string> out;
    out.push_back(value);

    CloneInfo cloneInfo("uuid1", "user1",
                     CloneTaskType::kClone, "src1",
                     "dst1", kDefaultPoolset, 1, 2, 3,
                     CloneFileType::kFile, false,
                     CloneStep::kCompleteCloneFile,
                     CloneStatus::cloning);

    std::string cloneValue;
    ASSERT_TRUE(codec.EncodeCloneInfoData(cloneInfo, &cloneValue));
    std::vector<std::string> cloneOut;
    cloneOut.push_back(cloneValue);

    EXPECT_CALL(*kvStorageClient_, List(_, _, Matcher<std::vector<std::string>*>(_)))  // NOLINT
        .WillOnce(DoAll(SetArgPointee<2>(out),
            Return(EtcdErrCode::EtcdOK)))
        .WillOnce(DoAll(SetArgPointee<2>(cloneOut),
            Return(EtcdErrCode::EtcdOK)));

    int ret = metaStore_->Init();
    ASSERT_EQ(0, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestInitListSnapshotFail) {
    EXPECT_CALL(*kvStorageClient_, List(_, _, Matcher<std::vector<std::string>*>(_)))  // NOLINT
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    int ret = metaStore_->Init();
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestInitListCloneInfoFail) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, "default", 0,
                        Status::pending);
    SnapshotCloneCodec codec;
    std::string value;
    ASSERT_TRUE(codec.EncodeSnapshotData(snapInfo, &value));
    std::vector<std::string> out;
    out.push_back(value);

    EXPECT_CALL(*kvStorageClient_, List(_, _, Matcher<std::vector<std::string>*>(_)))  // NOLINT
        .WillOnce(DoAll(SetArgPointee<2>(out),
            Return(EtcdErrCode::EtcdOK)))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    int ret = metaStore_->Init();
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestInitDecodeSnapshotFail) {
    std::vector<std::string> out;
    out.push_back("xxx");

    EXPECT_CALL(*kvStorageClient_, List(_, _, Matcher<std::vector<std::string>*>(_)))  // NOLINT
        .WillOnce(DoAll(SetArgPointee<2>(out),
            Return(EtcdErrCode::EtcdOK)));

    int ret = metaStore_->Init();
    ASSERT_EQ(-1, ret);
}

TEST_F(TestSnapshotCloneMetaStoreEtcd,
    TestInitDecodeCloneInfoFail) {
    SnapshotInfo snapInfo("snapuuid", "snapuser", "file1", "snapxxx", 100,
                        1024, 2048, 4096, 0, 0, "default", 0,
                        Status::pending);
    SnapshotCloneCodec codec;
    std::string value;
    ASSERT_TRUE(codec.EncodeSnapshotData(snapInfo, &value));
    std::vector<std::string> out;
    out.push_back(value);

    std::vector<std::string> out2;
    out2.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_, List(_, _, Matcher<std::vector<std::string>*>(_)))  // NOLINT
        .WillOnce(DoAll(SetArgPointee<2>(out),
            Return(EtcdErrCode::EtcdOK)))
        .WillOnce(DoAll(SetArgPointee<2>(out2),
            Return(EtcdErrCode::EtcdOK)));

    int ret = metaStore_->Init();
    ASSERT_EQ(-1, ret);
}




}  // namespace snapshotcloneserver
}  // namespace curve
