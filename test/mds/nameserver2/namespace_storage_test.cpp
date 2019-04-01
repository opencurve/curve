/*
 * Project: curve
 * Created Date: Thursday September 13th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <stdio.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/common/timeutility.h"
#include "test/mds/nameserver2/mock_etcdclient.h"

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
      storage_ = std::make_shared<NameServerStorageImp>(client_);
  }

  void TearDown() override {
      client_ = nullptr;
      storage_ = nullptr;
  }

  void GetFileInfoForTest(FileInfo *fileinfo) {
    std::string filename = "helloword-" + std::to_string(1) + ".log";
    fileinfo->set_id(1);
    fileinfo->set_filename(filename);
    fileinfo->set_parentid(1<<8);
    fileinfo->set_filetype(FileType::INODE_PAGEFILE);
    fileinfo->set_chunksize(DefaultChunkSize);
    fileinfo->set_length(10<<20);
    fileinfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    std::string fullpathname = "/A/B/" + std::to_string(1) + "/" + filename;
    fileinfo->set_fullpathname(fullpathname);
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
  std::shared_ptr<NameServerStorageImp> storage_;
};

TEST_F(TestNameServerStorageImp, test_putFile) {
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

TEST_F(TestNameServerStorageImp, test_getfile) {
    // 1. get file err
    FileInfo fileinfo;
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
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeFileinfo),
                  Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetFile(fileinfo.parentid(),
                                                 fileinfo.filename(),
                                                 &getInfo));
    ASSERT_EQ(fileinfo.filename(), getInfo.filename());
    ASSERT_EQ(fileinfo.fullpathname(), getInfo.fullpathname());
    ASSERT_EQ(fileinfo.parentid(), getInfo.parentid());
}

TEST_F(TestNameServerStorageImp, test_deletefile) {
    EXPECT_CALL(*client_, Delete(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::DeadlineExceeded));
    ASSERT_EQ(StoreStatus::OK, storage_->DeleteFile(1234, ""));
    ASSERT_EQ(StoreStatus::InternalError, storage_->DeleteFile(1234, ""));
}

TEST_F(TestNameServerStorageImp, test_renamefile) {
    EXPECT_CALL(*client_, Txn2(_, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    ASSERT_EQ(StoreStatus::OK,
        storage_->RenameFile(FileInfo{}, FileInfo{}));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->RenameFile(FileInfo{}, FileInfo{}));
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

TEST_F(TestNameServerStorageImp, test_putsegment) {
    PageFileSegment segment;
    EXPECT_CALL(*client_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Canceled));
    ASSERT_EQ(StoreStatus::OK, storage_->PutSegment(0, 0, &segment));
    ASSERT_EQ(StoreStatus::InternalError, storage_->PutSegment(0, 0, &segment));
}

TEST_F(TestNameServerStorageImp, test_getSegment) {
    // 1. get err
    PageFileSegment segment;
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
    EXPECT_CALL(*client_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encodeSegment),
                        Return(EtcdErrCode::OK)));
    ASSERT_EQ(StoreStatus::OK, storage_->GetSegment(0, 0, &getSegment));
    ASSERT_EQ(segment.chunksize(), getSegment.chunksize());
    ASSERT_EQ(segment.chunks_size(), getSegment.chunks_size());
}

TEST_F(TestNameServerStorageImp, test_deleteSegment) {
    EXPECT_CALL(*client_, Delete(_))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    ASSERT_EQ(StoreStatus::OK, storage_->DeleteSegment(0, 0));
    ASSERT_EQ(StoreStatus::InternalError, storage_->DeleteSegment(0, 0));
}

TEST_F(TestNameServerStorageImp, test_Snapshotfile) {
    EXPECT_CALL(*client_, Txn2(_, _))
        .WillOnce(Return(EtcdErrCode::OK))
        .WillOnce(Return(EtcdErrCode::Aborted));
    FileInfo fileinfo;
    fileinfo.set_filetype(FileType::INODE_PAGEFILE);
    ASSERT_EQ(StoreStatus::OK,
        storage_->SnapShotFile(&fileinfo, &fileinfo));
    ASSERT_EQ(StoreStatus::InternalError,
        storage_->SnapShotFile(&fileinfo, &fileinfo));
}

TEST(NameSpaceStorageTest, EncodeFileStoreKey) {
    std::string filename = "foo.txt";
    uint64_t parentID = 8;
    std::string str =
        NameSpaceStorageCodec::EncodeFileStoreKey(parentID, filename);

    ASSERT_EQ(str.size(), 17);
    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), FILEINFOKEYPREFIX);
    ASSERT_EQ(str.substr(10, filename.length()), filename);
    for (int i = 2;  i != 9; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[9]), 8);

    parentID = 8 << 8;
    str = NameSpaceStorageCodec::EncodeFileStoreKey(parentID, filename);

    ASSERT_EQ(str.size(), 17);
    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), FILEINFOKEYPREFIX);
    ASSERT_EQ(str.substr(10, filename.length()), filename);
    for (int i = 2;  i != 8; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[8]), 8);
    ASSERT_EQ(static_cast<int>(str[9]), 0);
}

TEST(NameSpaceStorageTest, EncodeSnapShotFileStoreKey) {
    std::string snapshotName = "hello-1";
    uint64_t parentID = 8;
    std::string str = NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(
                                                            parentID,
                                                            snapshotName);

    ASSERT_EQ(str.size(), 17);
    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), SNAPSHOTFILEINFOKEYPREFIX);
    ASSERT_EQ(str.substr(10, snapshotName.length()), snapshotName);
    for (int i = 2; i != 8; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[9]), 8);
}

TEST(NameSpaceStorageTest, EncodeSegmentStoreKey) {
    uint64_t inodeID = 8;
    offset_t offset = 3 << 16;
    std::string str =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(inodeID, offset);

    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), SEGMENTINFOKEYPREFIX);

    ASSERT_EQ(str.size(), 18);
    for (int i = 2;  i != 9; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[9]), 8);

    for (int i = 10;  i != 15; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[15]), 3);
    ASSERT_EQ(static_cast<int>(str[16]), 0);
    ASSERT_EQ(static_cast<int>(str[17]), 0);
}

TEST(NameSpaceStorageTest, test_EncodeAnDecode_FileInfo) {
    FileInfo fileInfo;
    fileInfo.set_id(2<<8);
    fileInfo.set_filename("helloword.log");
    fileInfo.set_parentid(1<<8);
    fileInfo.set_filetype(FileType::INODE_DIRECTORY);
    fileInfo.set_chunksize(DefaultChunkSize);
    fileInfo.set_length(10<<20);
    fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    fileInfo.set_fullpathname("/A/B/C/helloword.log");
    fileInfo.set_seqnum(1);

    // encode fileInfo
    std::string out;
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &out));
    ASSERT_EQ(fileInfo.ByteSize(), out.size());

    // decode fileInfo
    FileInfo decodeRes;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &decodeRes));
    ASSERT_EQ(fileInfo.id(), decodeRes.id());
    ASSERT_EQ(fileInfo.filename(), decodeRes.filename());
    ASSERT_EQ(fileInfo.parentid(), decodeRes.parentid());
    ASSERT_EQ(fileInfo.filetype(), decodeRes.filetype());
    ASSERT_EQ(fileInfo.chunksize(), decodeRes.chunksize());
    ASSERT_EQ(fileInfo.length(), decodeRes.length());
    ASSERT_EQ(fileInfo.ctime(), decodeRes.ctime());
    ASSERT_EQ(fileInfo.fullpathname(), decodeRes.fullpathname());
    ASSERT_EQ(fileInfo.seqnum(), decodeRes.seqnum());

    // encode fileInfo ctime donnot set
    fileInfo.clear_ctime();
    ASSERT_FALSE(fileInfo.has_ctime());
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &out));
    ASSERT_EQ(fileInfo.ByteSize(), out.size());

    // decode
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &decodeRes));
    ASSERT_FALSE(decodeRes.has_ctime());
}

TEST(NameSpaceStorageTest, test_EncodeAndDecode_Segment) {
    PageFileSegment segment;
    segment.set_chunksize(16<<20);
    segment.set_segmentsize(1 << 30);
    segment.set_startoffset(0);
    segment.set_logicalpoolid(16);
    int size = segment.segmentsize()/segment.chunksize();
    for (uint32_t i = 0; i < size; i++) {
        PageFileChunkInfo *chunkinfo = segment.add_chunks();
        chunkinfo->set_chunkid(i+1);
        chunkinfo->set_copysetid(i+1);
    }

    // encode segment
    std::string out;
    ASSERT_TRUE(NameSpaceStorageCodec::EncodeSegment(segment, &out));
    ASSERT_EQ(segment.ByteSize(), out.size());

    // decode segment
    PageFileSegment decodeRes;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeSegment(out, &decodeRes));
    ASSERT_EQ(segment.logicalpoolid(), decodeRes.logicalpoolid());
    ASSERT_EQ(segment.segmentsize(), decodeRes.segmentsize());
    ASSERT_EQ(segment.chunksize(), decodeRes.chunksize());
    ASSERT_EQ(segment.startoffset(), decodeRes.startoffset());
    ASSERT_EQ(segment.chunks_size(), decodeRes.chunks_size());
    for (int i = 0; i < size; i++) {
        ASSERT_EQ(i+1, decodeRes.chunks(i).chunkid());
        ASSERT_EQ(i+1, decodeRes.chunks(i).copysetid());
    }
}
}  // namespace mds
}  // namespace curve
