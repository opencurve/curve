/*
 * Project: curve
 * Created Date: Thursday September 13th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <stdio.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <memory>
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/timeutility.h"
#include "test/mds/mock/mock_etcdclient.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
class TestNameServerStorageImp : public ::testing::Test {
 protected:
    TestNameServerStorageImp() {}
    ~TestNameServerStorageImp() {}

    void SetUp() override {
        client_ = std::make_shared<MockEtcdClient>();
        cache_ = std::make_shared<MockLRUCache>();
        storage_ = std::make_shared<NameServerStorageImp>(client_, cache_);
    }

    void TearDown() override {
        client_ = nullptr;
        storage_ = nullptr;
    }

    void GetFileInfoForTest(FileInfo *fileinfo) {
        uint64_t DefaultChunkSize = 16 * kMB;
        std::string filename = "helloword-" + std::to_string(1) + ".log";
        fileinfo->set_id(1);
        fileinfo->set_filename(filename);
        fileinfo->set_parentid(1<<8);
        fileinfo->set_filetype(FileType::INODE_PAGEFILE);
        fileinfo->set_chunksize(DefaultChunkSize);
        fileinfo->set_length(10<<20);
        fileinfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());

        fileinfo->set_seqnum(1);
        std::string encodeFileInfo;
        ASSERT_TRUE(fileinfo->SerializeToString(&encodeFileInfo));
    }

    void GetPageFileSegmentForTest(
        std::string *fileKey, PageFileSegment *segment) {
        segment->set_chunksize(16<<20);
        segment->set_segmentsize(1 << 30);
        segment->set_startoffset(0);
        segment->set_logicalpoolid(16);
        int size = segment->segmentsize()/segment->chunksize();
        for (uint32_t i = 0; i < size; i++) {
            PageFileChunkInfo *chunkinfo = segment->add_chunks();
            chunkinfo->set_chunkid(i+1);
            chunkinfo->set_copysetid(i+1);
        }
        *fileKey = NameSpaceStorageCodec::EncodeSegmentStoreKey(1, 1);
    }

 protected:
    std::shared_ptr<MockEtcdClient> client_;
    std::shared_ptr<MockLRUCache> cache_;
    std::shared_ptr<NameServerStorageImp> storage_;
};

TEST_F(TestNameServerStorageImp, test_PutFile) {
    std::string storeKey;
    FileInfo fileinfo;
    GetFileInfoForTest(&fileinfo);
    EXPECT_CALL(*client_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Unknown))
        .WillOnce(Return(EtcdErrCode::InvalidArgument))
        .WillOnce(Return(EtcdErrCode::AlreadyExists))
        .WillOnce(Return(EtcdErrCode::PermissionDenied))
        .WillOnce(Return(EtcdErrCode::OutOfRange))
        .WillOnce(Return(EtcdErrCode::Unimplemented))
        .WillOnce(Return(EtcdErrCode::Internal))
        .WillOnce(Return(EtcdErrCode::NotFound))
        .WillOnce(Return(EtcdErrCode::DataLoss))
        .WillOnce(Return(EtcdErrCode::Unauthenticated))
        .WillOnce(Return(EtcdErrCode::Canceled))
        .WillOnce(Return(EtcdErrCode::DeadlineExceeded))
        .WillOnce(Return(EtcdErrCode::ResourceExhausted))
        .WillOnce(Return(EtcdErrCode::FailedPrecondition))
        .WillOnce(Return(EtcdErrCode::Aborted))
        .WillOnce(Return(EtcdErrCode::Unavailable))
        .WillOnce(Return(EtcdErrCode::TxnUnkownOp))
        .WillOnce(Return(EtcdErrCode::ObjectNotExist))
        .WillOnce(Return(EtcdErrCode::ErrObjectType));
    ASSERT_EQ(StoreStatus::OK, storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutFile(fileinfo));
}

TEST_F(TestNameServerStorageImp, test_GetFile) {
    // 1. get file err
    FileInfo fileinfo;
    EXPECT_CALL(*cache_, Get(_, _)).Times(2).WillRepeatedly(Return(false));
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(Return(EtcdErrCode::DeadlineExceeded))
        .WillOnce(Return(EtcdErrCode::KeyNotExist));
    ASSERT_EQ(StoreStatus::InternalError, storage_->GetFile(fileinfo.parentid(),
                                                            fileinfo.filename(),
                                                            &fileinfo));
    ASSERT_EQ(StoreStatus::KeyNotExist, storage_->GetFile(fileinfo.parentid(),
                                                          fileinfo.filename(),
                                                          &fileinfo));

    // 2. get file ok
    FileInfo getInfo;
    std::string encodeFileinfo;
    GetFileInfoForTest(&fileinfo);
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileinfo,
                                                      &encodeFileinfo));
    EXPECT_CALL(*cache_, Get(_, _)).WillOnce(Return(false));
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeFileinfo),
                  Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetFile(fileinfo.parentid(),
                                                 fileinfo.filename(),
                                                 &getInfo));
    ASSERT_EQ(fileinfo.filename(), getInfo.filename());
    ASSERT_EQ(fileinfo.parentid(), getInfo.parentid());

    // 3. get file from cache ok
    EXPECT_CALL(*cache_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeFileinfo), Return(true)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetFile(fileinfo.parentid(),
                                                 fileinfo.filename(),
                                                 &getInfo));
    ASSERT_EQ(fileinfo.filename(), getInfo.filename());
    ASSERT_EQ(fileinfo.parentid(), getInfo.parentid());
}

TEST_F(TestNameServerStorageImp, test_DeleteFile) {
    EXPECT_CALL(*client_, Delete(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::DeadlineExceeded));
    ASSERT_EQ(StoreStatus::OK, storage_->DeleteFile(1234, ""));
    ASSERT_EQ(StoreStatus::InternalError, storage_->DeleteFile(1234, ""));
}


TEST_F(TestNameServerStorageImp, test_DeleteSnapshotFile) {
    EXPECT_CALL(*client_, Delete(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::DeadlineExceeded));
    ASSERT_EQ(StoreStatus::OK,
        storage_->DeleteSnapshotFile(1234, ""));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->DeleteSnapshotFile(1234, ""));
}

TEST_F(TestNameServerStorageImp, test_RenameFile) {
    EXPECT_CALL(*client_, TxnN(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    ASSERT_EQ(StoreStatus::OK,
        storage_->RenameFile(FileInfo{}, FileInfo{}));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->RenameFile(FileInfo{}, FileInfo{}));
}

TEST_F(TestNameServerStorageImp, test_ReplaceFileAndRecycleOldFile) {
    FileInfo oldFileInfo;
    GetFileInfoForTest(&oldFileInfo);
    std::string oldFileInfoKey = NameSpaceStorageCodec::EncodeFileStoreKey(
        oldFileInfo.parentid(), oldFileInfo.filename());

    FileInfo existFileInfo;
    GetFileInfoForTest(&existFileInfo);
    existFileInfo.set_parentid(2<<8);
    existFileInfo.set_filename("exist.log");
    std::string existFileInfoKey = NameSpaceStorageCodec::EncodeFileStoreKey(
        existFileInfo.parentid(), existFileInfo.filename());

    FileInfo newFileInfo;
    newFileInfo.CopyFrom(oldFileInfo);
    newFileInfo.set_parentid(existFileInfo.parentid());
    newFileInfo.set_filename(existFileInfo.filename());
    std::string newFileInfoKey = NameSpaceStorageCodec::EncodeFileStoreKey(
        newFileInfo.parentid(), newFileInfo.filename());
    std::string encodeNewFileInfo;
    ASSERT_TRUE(newFileInfo.SerializeToString(&encodeNewFileInfo));

    FileInfo recycleFileInfo;
    recycleFileInfo.CopyFrom(existFileInfo);
    recycleFileInfo.set_filestatus(FileStatus::kFileDeleting);
    recycleFileInfo.set_filetype(INODE_PAGEFILE);
    std::string recycleFileInfoKey =
        NameSpaceStorageCodec::EncodeFileStoreKey(
        recycleFileInfo.parentid(), recycleFileInfo.filename());
    std::string encoderecycleFileInfo;
    ASSERT_TRUE(recycleFileInfo.SerializeToString(&encoderecycleFileInfo));

    EXPECT_CALL(*client_, TxnN(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    ASSERT_EQ(StoreStatus::OK,
        storage_->ReplaceFileAndRecycleOldFile(
            oldFileInfo, newFileInfo, existFileInfo, recycleFileInfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->ReplaceFileAndRecycleOldFile(
            oldFileInfo, newFileInfo, existFileInfo, recycleFileInfo));
}

TEST_F(TestNameServerStorageImp, test_MoveFileToRecycle) {
    FileInfo originFileInfo;
    GetFileInfoForTest(&originFileInfo);
    std::string originFileInfoKey = NameSpaceStorageCodec::EncodeFileStoreKey(
        originFileInfo.parentid(), originFileInfo.filename());

    FileInfo recycleFileInfo;
    recycleFileInfo.CopyFrom(originFileInfo);
    recycleFileInfo.set_filestatus(FileStatus::kFileDeleting);
    recycleFileInfo.set_filetype(INODE_PAGEFILE);
    std::string recycleFileInfoKey =
        NameSpaceStorageCodec::EncodeFileStoreKey(
        recycleFileInfo.parentid(), recycleFileInfo.filename());
    std::string encoderecycleFileInfo;
    ASSERT_TRUE(recycleFileInfo.SerializeToString(&encoderecycleFileInfo));

    EXPECT_CALL(*client_, TxnN(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    ASSERT_EQ(StoreStatus::OK,
        storage_->MoveFileToRecycle(originFileInfo, recycleFileInfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->MoveFileToRecycle(originFileInfo, recycleFileInfo));
}

TEST_F(TestNameServerStorageImp, test_ListFile) {
    // 1. list err
    std::vector<FileInfo> listRes;
    EXPECT_CALL(*client_, List(_, _, _))
        .WillOnce(Return(EtcdErrCode::Canceled));
    ASSERT_EQ(StoreStatus::InternalError, storage_->ListFile(0, 0, &listRes));

    // 2. list ok
    listRes.clear();
    std::string encodeFileinfo;
    FileInfo fileinfo;
    GetFileInfoForTest(&fileinfo);
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileinfo,
                                                      &encodeFileinfo));
    EXPECT_CALL(*client_, List(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(std::vector<std::string>{encodeFileinfo}),
            Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->ListFile(0, 0, &listRes));
    ASSERT_EQ(1, listRes.size());
    ASSERT_EQ(fileinfo.filename(), listRes[0].filename());
    ASSERT_EQ(fileinfo.seqnum(), listRes[0].seqnum());
}

TEST_F(TestNameServerStorageImp, test_ListSnapshotFile) {
    // 1. list err
    std::vector<FileInfo> listRes;
    EXPECT_CALL(*client_, List(_, _, _))
        .WillOnce(Return(EtcdErrCode::Canceled));
    ASSERT_EQ(
        StoreStatus::InternalError, storage_->ListSnapshotFile(1, 2, &listRes));

    // 2. list ok
    listRes.clear();
    std::string encodeFileinfo;
    FileInfo fileinfo;
    GetFileInfoForTest(&fileinfo);
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileinfo,
                                                      &encodeFileinfo));
    std::string startStoreKey =
        NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(1, "");
    std::string endStoreKey =
        NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(2, "");

    EXPECT_CALL(*client_, List(startStoreKey, endStoreKey, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(std::vector<std::string>{encodeFileinfo}),
            Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->ListSnapshotFile(1, 2, &listRes));
    ASSERT_EQ(1, listRes.size());
    ASSERT_EQ(fileinfo.filename(), listRes[0].filename());
    ASSERT_EQ(fileinfo.seqnum(), listRes[0].seqnum());
}

TEST_F(TestNameServerStorageImp, test_putsegment) {
    PageFileSegment segment;
    segment.set_segmentsize(1024*1024*1024);
    segment.set_chunksize(16*1024*1024);
    segment.set_startoffset(0);
    segment.set_logicalpoolid(1);
    EXPECT_CALL(*client_, PutRewithRevision(_, _, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Canceled));
    int64_t revision;
    ASSERT_EQ(StoreStatus::OK, storage_->PutSegment(0, 0, &segment, &revision));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->PutSegment(0, 0, &segment, &revision));
}

TEST_F(TestNameServerStorageImp, test_getSegment) {
    // 1. get err
    PageFileSegment segment;
    EXPECT_CALL(*cache_, Get(_, _)).Times(2).WillRepeatedly(Return(false));
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(Return(EtcdErrCode::Canceled))
        .WillOnce(Return(EtcdErrCode::KeyNotExist));
    ASSERT_EQ(StoreStatus::InternalError, storage_->GetSegment(0, 0, &segment));
    ASSERT_EQ(StoreStatus::KeyNotExist, storage_->GetSegment(0, 0, &segment));

    // 2. get ok
    PageFileSegment getSegment;
    std::string key, encodeSegment;
    GetPageFileSegmentForTest(&key, &segment);
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
    EXPECT_CALL(*cache_, Get(_, _)).WillOnce(Return(false));
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeSegment),
                        Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetSegment(0, 0, &getSegment));
    ASSERT_EQ(segment.chunksize(), getSegment.chunksize());
    ASSERT_EQ(segment.chunks_size(), getSegment.chunks_size());

    // 3. get file from cache ok
    EXPECT_CALL(*cache_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeSegment), Return(true)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetSegment(0, 0, &getSegment));
    ASSERT_EQ(segment.chunksize(), getSegment.chunksize());
    ASSERT_EQ(segment.chunks_size(), getSegment.chunks_size());
}

TEST_F(TestNameServerStorageImp, test_deleteSegment) {
    EXPECT_CALL(*client_, DeleteRewithRevision(_, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    int64_t revision;
    ASSERT_EQ(StoreStatus::OK, storage_->DeleteSegment(0, 0, &revision));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->DeleteSegment(0, 0, &revision));
}

TEST_F(TestNameServerStorageImp, test_Snapshotfile) {
    EXPECT_CALL(*client_, TxnN(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    FileInfo fileinfo;
    fileinfo.set_filetype(FileType::INODE_PAGEFILE);
    ASSERT_EQ(StoreStatus::OK,
        storage_->SnapShotFile(&fileinfo, &fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->SnapShotFile(&fileinfo, &fileinfo));
}

}  // namespace mds
}  // namespace curve
