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
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"


#include "test/mds/nameserver2/mock/mock_namespace_storage.h"
#include "test/mds/nameserver2/mock/mock_inode_id_generator.h"
#include "test/mds/nameserver2/mock/mock_chunk_allocate.h"
#include "test/mds/nameserver2/mock/mock_clean_manager.h"
#include "test/mds/nameserver2/mock/mock_snapshotclone_client.h"
#include "test/mds/nameserver2/mock/mock_file_record_manager.h"
#include "test/mds/mock/mock_alloc_statistic.h"
#include "test/mds/mock/mock_topology.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using curve::common::Authenticator;

using curve::common::TimeUtility;
using curve::mds::topology::MockTopology;
using curve::mds::snapshotcloneclient::MockSnapshotCloneClient;
using curve::mds::snapshotcloneclient::DestFileInfo;

namespace curve {
namespace mds {

class CurveFSTest: public ::testing::Test {
 protected:
    void SetUp() override {
        storage_ = std::make_shared<MockNameServerStorage>();
        inodeIdGenerator_ = std::make_shared<MockInodeIDGenerator>();
        mockChunkAllocator_ = std::make_shared<MockChunkAllocator>();

        mockcleanManager_ = std::make_shared<MockCleanManager>();
        topology_ = std::make_shared<MockTopology>();
        snapshotClient_ = std::make_shared<MockSnapshotCloneClient>();
        // session repo已经mock，数据库相关参数不需要
        fileRecordManager_ = std::make_shared<MockFileRecordManager>();
        fileRecordOptions_.fileRecordExpiredTimeUs = 5 * 1000;
        fileRecordOptions_.scanIntervalTimeUs = 1 * 1000;

        authOptions_.rootOwner = "root";
        authOptions_.rootPassword = "root_password";

        curveFSOptions_.defaultChunkSize = 16 * kMB;
        curveFSOptions_.authOptions = authOptions_;
        curveFSOptions_.fileRecordOptions = fileRecordOptions_;

        curvefs_ =  &kCurveFS;

        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        FileInfo fileInfo;
        fileInfo.set_parentid(ROOTINODEID);
        fileInfo.set_id(RECYCLEBININODEID);
        fileInfo.set_filename(RECYCLEBINDIRNAME);
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo.set_owner(authOptions_.rootOwner);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                Return(StoreStatus::OK)));

        curvefs_->Init(storage_, inodeIdGenerator_, mockChunkAllocator_,
                        mockcleanManager_,
                        fileRecordManager_,
                        allocStatistic_,
                        curveFSOptions_,
                        topology_,
                        snapshotClient_);
        curvefs_->Run();
    }

    void TearDown() override {
        curvefs_->Uninit();
    }

    CurveFS *curvefs_;
    std::shared_ptr<MockNameServerStorage> storage_;
    std::shared_ptr<MockInodeIDGenerator> inodeIdGenerator_;
    std::shared_ptr<MockChunkAllocator> mockChunkAllocator_;

    std::shared_ptr<MockCleanManager> mockcleanManager_;
    std::shared_ptr<MockFileRecordManager> fileRecordManager_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<MockSnapshotCloneClient> snapshotClient_;
    struct FileRecordOptions fileRecordOptions_;
    struct RootAuthOption authOptions_;
    struct CurveFSOption curveFSOptions_;
};

TEST_F(CurveFSTest, testCreateFile1) {
    // test parm error
    ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1", FileType::INODE_PAGEFILE,
                    kMiniFileLength - 1, 0, 0),
                    StatusCode::kFileLengthNotSupported);

    ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1", FileType::INODE_PAGEFILE,
                    kMaxFileLength + 1, 0, 0),
                    StatusCode::kFileLengthNotSupported);

    ASSERT_EQ(curvefs_->CreateFile("/flie1", "owner1", FileType::INODE_PAGEFILE,
                                   kMiniFileLength + 1, 0, 0),
              StatusCode::kFileLengthNotSupported);

    ASSERT_EQ(curvefs_->CreateFile("/", "", FileType::INODE_DIRECTORY, 0, 0, 0),
              StatusCode::kFileExists);

    {
        // test file exist
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        auto statusCode = curvefs_->CreateFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, 0, 0);
        ASSERT_EQ(statusCode, StatusCode::kFileExists);
    }

    {
        // test get storage error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        auto statusCode = curvefs_->CreateFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, 0, 0);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test put storage error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));

        auto statusCode = curvefs_->CreateFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, 0, 0);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test put storage ok
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));


        auto statusCode = curvefs_->CreateFile("/file1", "owner1",
            FileType::INODE_PAGEFILE, kMiniFileLength, 0, 0);
        ASSERT_EQ(statusCode, StatusCode::kOK);
    }

    {
        // test inode allocate error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(false));

        auto statusCode = curvefs_->CreateFile("/file1", "owner1",
                FileType::INODE_PAGEFILE, kMiniFileLength, 0, 0);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }
}

TEST_F(CurveFSTest, testCreateStripeFile) {
    {
        // test create ok
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));

        ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1",
                  FileType::INODE_PAGEFILE, kMiniFileLength,
                  1 * 1024 * 1024, 4), StatusCode::kOK);
    }

    {
        // test stripeStripe and stripeCount is not all zero
        ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1",
                   FileType::INODE_PAGEFILE, kMiniFileLength, 0, 1),
                    StatusCode::kParaError);
        ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, 1024*1024ul, 0),
                                   StatusCode::kParaError);
    }

    {
        // test stripeUnit more then chunksize
        ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1",
        FileType::INODE_PAGEFILE, kMiniFileLength, 16*1024*1024ul + 1, 0),
                    StatusCode::kParaError);
    }

    {
        // test stripeUnit is not divisible by chunksize
        ASSERT_EQ(curvefs_->CreateFile("/file1", "owner1",
            FileType::INODE_PAGEFILE,  kMiniFileLength,
            4*1024*1024ul + 1, 0), StatusCode::kParaError);
    }
}

TEST_F(CurveFSTest, testGetFileInfo) {
    // test parm error
    FileInfo fileInfo;
    auto ret = curvefs_->GetFileInfo("/", &fileInfo);
    ASSERT_EQ(ret, StatusCode::kOK);

    FileInfo rootFileInfo = curvefs_->GetRootFileInfo();
    ASSERT_EQ(fileInfo.id(), rootFileInfo.id());
    ASSERT_EQ(fileInfo.filename(),  rootFileInfo.filename());
    ASSERT_EQ(fileInfo.filetype(), rootFileInfo.filetype());

    {
        // test path not exist
        FileInfo  fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kFileNotExists);
    }
    {
        // test stoarge error
        FileInfo fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));
        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kStorageError);
    }
    {
        // test  ok
        FileInfo fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kOK);
    }
    {
        // test  WalkPath NOT DIRECTORY
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        FileInfo retFileInfo;
        std::string lastEntry;
        ASSERT_EQ(curvefs_->GetFileInfo("/testdir/file1", &retFileInfo),
            StatusCode::kFileNotExists);
    }
    {
        // test LookUpFile internal Error
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo fileInfo1;
        ASSERT_EQ(curvefs_->GetFileInfo("testdir/file1", &fileInfo1),
            StatusCode::kStorageError);
    }
}

TEST_F(CurveFSTest, testDeleteFile) {
    // test remove root
    ASSERT_EQ(curvefs_->DeleteFile("/", kUnitializedFileID, false),
                                                StatusCode::kParaError);

    // test delete directory ok
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/dir1", kUnitializedFileID, false),
                                                    StatusCode::kOK);
    }

    // test delete directory, directory is not empty
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        fileInfoList.push_back(fileInfo);
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->DeleteFile("/dir1", kUnitializedFileID, false),
                                        StatusCode::kDirNotEmpty);
    }

    // test delete directory, delete file fail
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->DeleteFile("/dir1", kUnitializedFileID, false),
                                            StatusCode::kStorageError);
    }

    // test delete pagefile ok
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                                    StatusCode::kOK);
    }

    // test delete recyclebin pagefile，cleanManager fail
    {
        FileInfo recycleBindir;
        recycleBindir.set_parentid(ROOTINODEID);
        recycleBindir.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo  fileInfo;
        fileInfo.set_parentid(RECYCLEBININODEID);
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*mockcleanManager_,
            GetTask(_))
            .Times(1)
            .WillOnce(Return(nullptr));

        EXPECT_CALL(*mockcleanManager_,
            SubmitDeleteCommonFileJob(_))
        .Times(1)
        .WillOnce(Return(false));

        ASSERT_EQ(curvefs_->DeleteFile(RECYCLEBINDIR + "/file1",
                                        kUnitializedFileID, true),
            StatusCode::KInternalError);
    }

    // test force delete recyclebin file ok
    {
        FileInfo recycleBindir;
        recycleBindir.set_parentid(ROOTINODEID);
        recycleBindir.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo  fileInfo;
        fileInfo.set_parentid(RECYCLEBININODEID);
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*mockcleanManager_,
            GetTask(_))
            .Times(1)
            .WillOnce(Return(nullptr));

        EXPECT_CALL(*mockcleanManager_,
            SubmitDeleteCommonFileJob(_))
        .Times(1)
        .WillOnce(Return(true));

        ASSERT_EQ(curvefs_->DeleteFile(RECYCLEBINDIR + "/file1",
                                    kUnitializedFileID, true),
            StatusCode::kOK);
    }

    // test force delete already deleting
    {
        FileInfo recycleBindir;
        recycleBindir.set_parentid(ROOTINODEID);
        recycleBindir.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo  fileInfo;
        fileInfo.set_parentid(RECYCLEBININODEID);
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(recycleBindir),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        // mockcleanManager_ = std::make_shared<MockCleanManager>();
        auto notNullTask =
            std::make_shared<SnapShotCleanTask>(1, nullptr, fileInfo);
        EXPECT_CALL(*mockcleanManager_,
            GetTask(_))
            .Times(1)
            .WillOnce(Return(notNullTask));

        ASSERT_EQ(curvefs_->DeleteFile(RECYCLEBINDIR + "/file1",
                                            kUnitializedFileID, true),
            StatusCode::kOK);
    }

     // test force delete file not in recyclebin
    {
        FileInfo  fileInfo;
        fileInfo.set_parentid(USERSTARTINODEID);
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, true),
            StatusCode::kNotSupported);
    }
    // test delete pagefile, file under snapshot
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        fileInfoList.push_back(fileInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                StatusCode::kFileUnderSnapShot);
    }

    // test delete pagefile, storage error
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kStorageError);
    }

    //  test file not exist
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                            StatusCode::kFileNotExists);
    }

    // delete not support file type
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_APPENDFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                                StatusCode::kNotSupported);
    }

    // test delete pagefile, file under clone
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kHasRef;
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            Return(StatusCode::kOK)));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                StatusCode::kDeleteFileBeingCloned);
    }

    // test delete pagefile, file under clone but has no ref but delete fail
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNoRef;
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            Return(StatusCode::kOK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kStorageError);
    }

    // test delete pagefile, file under clone but has no ref success
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNoRef;
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            Return(StatusCode::kOK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kOK);
    }

    // test delete pagefile, file under clone but need check list empty
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNeedCheck;
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            Return(StatusCode::kOK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kOK);
    }

    // test delete pagefile, file under clone but need check, file has ref
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::KeyNotExist)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNeedCheck;
        std::vector<DestFileInfo> fileCheckList;
        DestFileInfo info;
        info.filename = "/file";
        info.inodeid = 100;
        fileCheckList.push_back(info);
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            SetArgPointee<3>(fileCheckList),
            Return(StatusCode::kOK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kOK);
    }

    // test delete pagefile, file under clone but need check, inode mismatch
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        fileInfo.set_id(10);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNeedCheck;
        std::vector<DestFileInfo> fileCheckList;
        DestFileInfo info;
        info.filename = "/file";
        info.inodeid = 100;
        fileCheckList.push_back(info);
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            SetArgPointee<3>(fileCheckList),
            Return(StatusCode::kOK)));

        EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kOK);
    }

    // test delete pagefile, file under clone but need check, has ref
    {
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileBeingCloned);
        fileInfo.set_id(100);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        CloneRefStatus status = CloneRefStatus::kNeedCheck;
        std::vector<DestFileInfo> fileCheckList;
        DestFileInfo info;
        info.filename = "/file";
        info.inodeid = 100;
        fileCheckList.push_back(info);
        EXPECT_CALL(*snapshotClient_, GetCloneRefStatus(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(status),
            SetArgPointee<3>(fileCheckList),
            Return(StatusCode::kOK)));

        // EXPECT_CALL(*storage_, MoveFileToRecycle(_, _))
        // .Times(1)
        // .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                                     StatusCode::kDeleteFileBeingCloned);
    }

    // test delete failed when mds didn't start for enough time
    {
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<2>(fileInfo), Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
            .Times(1)
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfoList), Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(50 * 1000000ul));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                  StatusCode::kNotSupported);
    }

    // test delete failed when file is in-use
    {
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<2>(fileInfo), Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
            .Times(1)
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfoList), Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        ClientIpPortType mountPoint("127.0.0.1", 12345);
        EXPECT_CALL(*fileRecordManager_, FindFileMountPoint(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(mountPoint), Return(true)));

        ASSERT_EQ(curvefs_->DeleteFile("/file1", kUnitializedFileID, false),
                  StatusCode::kFileOccupied);
    }
}

TEST_F(CurveFSTest, testGetAllocatedSize) {
    AllocatedSize allocSize;
    FileInfo  fileInfo;
    uint64_t segmentSize = 1 * 1024 * 1024 * 1024ul;
    fileInfo.set_id(0);
    fileInfo.set_filetype(FileType::INODE_PAGEFILE);
    fileInfo.set_segmentsize(segmentSize);
    std::vector<PageFileSegment> segments;
    PageFileSegment segment;
    for (int i = 0; i < 2; ++i) {
        segment.set_logicalpoolid(1);
        segment.set_segmentsize(segmentSize);
        segment.set_chunksize(curvefs_->GetDefaultChunkSize());
        segment.set_startoffset(i);
        segments.emplace_back(segment);
    }
    segment.set_logicalpoolid(2);
    segments.emplace_back(segment);


    // test page file normal
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListSegment(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(segments),
            Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kOK,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
        ASSERT_EQ(3 * segmentSize, allocSize.total);
        std::unordered_map<PoolIdType, uint64_t> expected =
                        {{1, 2 * segmentSize}, {2, segmentSize}};
        ASSERT_EQ(expected, allocSize.allocSizeMap);
    }
    // test directory normal
    {
        FileInfo dirInfo;
        dirInfo.set_filetype(FileType::INODE_DIRECTORY);
        std::vector<FileInfo> files;
        for (int i = 0; i < 3; ++i) {
            files.emplace_back(fileInfo);
        }
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(dirInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(files),
                        Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListSegment(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(segments),
            Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kOK,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
        ASSERT_EQ(9 * segmentSize, allocSize.total);
        std::unordered_map<PoolIdType, uint64_t> expected =
                        {{1, 6 * segmentSize}, {2, 3 * segmentSize}};
    }
    // test GetFile fail
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(StatusCode::kFileNotExists,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
    }
    // test file type not supported
    {
        FileInfo appendFileInfo;
        appendFileInfo.set_filetype(INODE_APPENDFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(appendFileInfo),
            Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kNotSupported,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
    }
    // test list segment fail
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListSegment(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));
        ASSERT_EQ(StatusCode::kStorageError,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
    }
    // test list directory fail
    {
        FileInfo dirInfo;
        dirInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(dirInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));
        ASSERT_EQ(StatusCode::kStorageError,
                    curvefs_->GetAllocatedSize("/tests", &allocSize));
    }
}

TEST_F(CurveFSTest, testGetFileSize) {
    uint64_t fileSize;
    FileInfo  fileInfo;
    fileInfo.set_id(0);
    fileInfo.set_filetype(FileType::INODE_PAGEFILE);
    fileInfo.set_length(10 * kGB);

    // test page file normal
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kOK,
                    curvefs_->GetFileSize("/tests", &fileSize));
        ASSERT_EQ(10 * kGB, fileSize);
    }
    // test directory normal
    {
        FileInfo dirInfo;
        dirInfo.set_filetype(FileType::INODE_DIRECTORY);
        std::vector<FileInfo> files;
        for (int i = 0; i < 3; ++i) {
            files.emplace_back(fileInfo);
        }
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(dirInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(files),
                        Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kOK,
                    curvefs_->GetFileSize("/tests", &fileSize));
        ASSERT_EQ(30 * kGB, fileSize);
    }
    // test GetFile fail
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(StatusCode::kFileNotExists,
                    curvefs_->GetFileSize("/tests", &fileSize));
    }
    // test file type not supported
    {
        FileInfo appendFileInfo;
        appendFileInfo.set_filetype(INODE_APPENDFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(appendFileInfo),
            Return(StoreStatus::OK)));
        ASSERT_EQ(StatusCode::kNotSupported,
                    curvefs_->GetFileSize("/tests", &fileSize));
    }
    // test list directory fail
    {
        FileInfo dirInfo;
        dirInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(dirInfo),
            Return(StoreStatus::OK)));
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));
        ASSERT_EQ(StatusCode::kStorageError,
                    curvefs_->GetFileSize("/tests", &fileSize));
    }
}

TEST_F(CurveFSTest, testReadDir) {
    FileInfo fileInfo;
    std::vector<FileInfo> items;

    // test not directory
    {
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ReadDir("/file1", &items),
                  StatusCode::kNotDirectory);
        items.clear();
    }

    // test getFile Not exist
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->ReadDir("/file1", &items),
                  StatusCode::kDirNotExist);
    }

    // test listFile ok
    {
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)));

        std::vector<FileInfo> sideEffectArgs;
        sideEffectArgs.clear();
        sideEffectArgs.push_back(fileInfo);
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        sideEffectArgs.push_back(fileInfo);

        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(sideEffectArgs),
                        Return(StoreStatus::OK)));

        auto ret = curvefs_->ReadDir("/file1", &items);
        ASSERT_EQ(ret, StatusCode::kOK);
        ASSERT_EQ(items.size(), 2);
        ASSERT_EQ(items[0].filetype(), INODE_DIRECTORY);
        ASSERT_EQ(items[1].filetype(), INODE_PAGEFILE);
    }
}


TEST_F(CurveFSTest, testRenameFile) {
    // test rename ok
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(4))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, RenameFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kOK);
    }

    // old file is under snapshot, can not rename
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        std::vector<FileInfo> snapshotFileInfos;
        snapshotFileInfos.push_back(fileInfo1);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapshotFileInfos),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kFileUnderSnapShot);
    }

    // old file not exist
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kFileNotExists);
    }

    // new file parent directory not exist
    {
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kFileNotExists);
    }

    // storage renamefile fail
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(4))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, RenameFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kStorageError);
    }

    // rename same file
    {
        ASSERT_EQ(curvefs_->RenameFile("/file1", "/file1", 0, 0),
                  StatusCode::kFileExists);
    }

    // rename root
    {
        ASSERT_EQ(curvefs_->RenameFile("/", "/file1", 0, 0),
                  StatusCode::kParaError);
    }
    // rename to root
    {
        ASSERT_EQ(curvefs_->RenameFile("/file1", "/", 0, 0),
                  StatusCode::kParaError);
    }

    // rename dir
    {
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kNotSupported);
    }

    // new file exist, rename success
    {
        uint64_t fileId = 10;
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        FileInfo fileInfo3;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo3.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_id(10);
        fileInfo3.set_id(11);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(6)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo3),
                        Return(StoreStatus::OK)))
        // check /trash/file2是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo3),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(3)
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ReplaceFileAndRecycleOldFile(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 10, 11),
                  StatusCode::kOK);
    }

    // new file exist, filetype mismatch
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(4)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kFileExists);
    }

    // new file exist, new file under snapshot
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(6)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /trash/file2是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        std::vector<FileInfo> snapshotFileInfos;
        snapshotFileInfos.push_back(fileInfo1);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(DoAll(SetArgPointee<2>(snapshotFileInfos),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kFileUnderSnapShot);
    }

    // new file exist, mv to recycle fail
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(6)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /trash/file2是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ReplaceFileAndRecycleOldFile(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kStorageError);
    }

    // new file exist, rename fail
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(6)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /trash/file2是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ReplaceFileAndRecycleOldFile(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kStorageError);
    }

    // new file exist, submit delete job fail
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(6)
        // 查找/file1
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /file1是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // 查找/trash/file2
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        // check /trash/file2是否有快照
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::KeyNotExist))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, ReplaceFileAndRecycleOldFile(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2", 0, 0),
                  StatusCode::kOK);
    }

    // new file exist, and new file is in-use(file with session record)
    {
        FileInfo fileInfo1;
        FileInfo fileInfo2;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(6)
            // 查找/file1
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo1), Return(StoreStatus::OK)))
            // check /file1是否有快照
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo1), Return(StoreStatus::OK)))
            // 查找/trash/file2
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo2), Return(StoreStatus::OK)))
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo1), Return(StoreStatus::OK)))
            // check /trash/file2是否有快照
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo2), Return(StoreStatus::OK)))
            .WillOnce(
                DoAll(SetArgPointee<2>(fileInfo1), Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(2)
            .WillRepeatedly(Return(1ul));

        ClientIpPortType mountPoint("127.0.0.1", 12345);
        EXPECT_CALL(*fileRecordManager_, FindFileMountPoint(_, _))
            .Times(2)
            .WillOnce(Return(false))
            .WillOnce(DoAll(SetArgPointee<1>(mountPoint), Return(true)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
            .Times(2)
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(StatusCode::kFileOccupied,
                  curvefs_->RenameFile("/file1", "/trash/file2", 0, 0));
    }
}

TEST_F(CurveFSTest, testExtendFile) {
    // test try small filesize && same
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);


        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1", 0),
                  StatusCode::kShrinkBiggerFile);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
                                       kMiniFileLength), StatusCode::kOK);
    }

    // test enlarge size unit is not segment
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
            1 + kMiniFileLength), StatusCode::kExtentUnitError);
    }

    // test enlarge size ok
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
                                       2 * kMiniFileLength), StatusCode::kOK);
    }

    // test size over maxsize
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
                    2 * kMaxFileLength), StatusCode::kFileLengthNotSupported);
    }

    // file not exist
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
                                       2 * kMiniFileLength),
                                       StatusCode::kFileNotExists);
    }

    // extend directory
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtendFile("/user1/file1",
                                       2 * kMiniFileLength),
                                       StatusCode::kNotSupported);
    }
}

TEST_F(CurveFSTest, testChangeOwner) {
    // test changeOwner ok
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kOK);
    }

    // file owner same with newowner
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner1"),
                  StatusCode::kOK);
    }

    // file is under snapshot, can not changeOwner
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        std::vector<FileInfo> snapshotFileInfos;
        snapshotFileInfos.push_back(fileInfo1);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapshotFileInfos),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kFileUnderSnapShot);
    }

    // file not exist
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kFileNotExists);
    }

    // storage putfile fail
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kStorageError);
    }

    // changeOwner dir ok
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kOK);
    }

    // changeOwner dir not empty
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        std::vector<FileInfo> fileInfoList;
        fileInfoList.push_back(fileInfo1);
        EXPECT_CALL(*storage_, ListFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfoList),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kDirNotEmpty);
    }

    // filetype mismatch
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_APPENDECFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kNotSupported);
    }

    // ChangeOwner file is in-use
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo1.set_owner("owner1");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<2>(fileInfo1), Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
            .Times(1)
            .WillOnce(Return(StoreStatus::KeyNotExist));

        ClientIpPortType mountPoint("127.0.0.1", 12345);
        EXPECT_CALL(*fileRecordManager_, FindFileMountPoint(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(mountPoint), Return(true)));

        ASSERT_EQ(curvefs_->ChangeOwner("/file1", "owner2"),
                  StatusCode::kFileOccupied);
    }
}

TEST_F(CurveFSTest, testGetOrAllocateSegment) {
    // test normal get exist segment
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, false,  &segment), StatusCode::kOK);
    }

    // test normal get & allocate not exist segment
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));


        EXPECT_CALL(*mockChunkAllocator_, AllocateChunkSegment(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(true));


        EXPECT_CALL(*storage_, PutSegment(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, true,  &segment), StatusCode::kOK);
    }

    // file is a directory
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_DIRECTORY);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, false,  &segment), StatusCode::kParaError);
    }

    // segment offset not align file segment size
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  1, false,  &segment), StatusCode::kParaError);
    }

    // file length < segment offset + segmentsize
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  kMiniFileLength, false,  &segment), StatusCode::kParaError);
    }

    // alloc chunk segment fail
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));


        EXPECT_CALL(*mockChunkAllocator_, AllocateChunkSegment(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(false));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, true,  &segment), StatusCode::kSegmentAllocateError);
    }

    // put segment fail
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));


        EXPECT_CALL(*mockChunkAllocator_, AllocateChunkSegment(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(true));


        EXPECT_CALL(*storage_, PutSegment(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, true,  &segment), StatusCode::kStorageError);
    }
}

TEST_F(CurveFSTest, testCreateSnapshotFile) {
    {
        // test client time not expired
        FileInfo originalFile;
        originalFile.set_seqnum(1);
        originalFile.set_filetype(FileType::INODE_PAGEFILE);
        std::string fileName = "/snapshotFile1WithInvalidClientVersion";

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(originalFile),
                            Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(50 * 1000000ul));

        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(StatusCode::kSnapshotFrozen,
                  curvefs_->CreateSnapShotFile(fileName, &snapShotFileInfoRet));
    }
    {
        // test client version invalid
        std::string fileName = "/snapshotFile1WithInvalidClientVersion2";
        std::this_thread::sleep_for(std::chrono::microseconds(
            11 * fileRecordOptions_.fileRecordExpiredTimeUs));
        FileInfo originalFile;
        originalFile.set_seqnum(1);
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::OK))
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        FileInfo info;
        ASSERT_EQ(StatusCode::kOK,
            curvefs_->RefreshSession(
                fileName, "", 0 , "", "",  1234, "0.0.5", &info));

        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(curvefs_->CreateSnapShotFile(
                "/snapshotFile1WithInvalidClientVersion2",
                &snapShotFileInfoRet), StatusCode::kClientVersionNotMatch);
    }
    {
        // test client version empty invalid
        std::string fileName = "/snapshotFile1WithInvalidClientVersion2";
        FileInfo originalFile;
        originalFile.set_seqnum(1);
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(Return(StoreStatus::OK))
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        FileInfo info;
        ASSERT_EQ(StatusCode::kOK,
            curvefs_->RefreshSession(
                fileName, "", 0 , "", "",  1234, "", &info));

        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(curvefs_->CreateSnapShotFile(
                "/snapshotFile1WithInvalidClientVersion2",
                &snapShotFileInfoRet), StatusCode::kClientVersionNotMatch);
    }
    {
        // test under snapshot
        FileInfo originalFile;
        originalFile.set_seqnum(1);
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        std::vector<FileInfo> snapShotFiles;
        FileInfo fileInfo1;
        snapShotFiles.push_back(fileInfo1);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(curvefs_->CreateSnapShotFile("/snapshotFile1",
                &snapShotFileInfoRet), StatusCode::kFileUnderSnapShot);
    }
    {
        // test File is not PageFile
    }
    {
        // test storage ListFile error
    }
    {
        // test GenId error
    }
    {
        // test create snapshot ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(2),
            Return(true)));

        FileInfo snapShotFileInfo;
        EXPECT_CALL(*storage_, SnapShotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        // test client version valid
        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(curvefs_->CreateSnapShotFile("/originalFile",
                &snapShotFileInfoRet), StatusCode::kOK);
        ASSERT_EQ(snapShotFileInfoRet.parentid(), originalFile.id());
        ASSERT_EQ(snapShotFileInfoRet.filename(),
            originalFile.filename() + "-" +
            std::to_string(originalFile.seqnum()) );
        ASSERT_EQ(snapShotFileInfoRet.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(
            snapShotFileInfoRet.filetype(), FileType::INODE_SNAPSHOT_PAGEFILE);
    }
    {
        // test create snapshot ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile2");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(2),
            Return(true)));

        FileInfo snapShotFileInfo;
        EXPECT_CALL(*storage_, SnapShotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*fileRecordManager_, GetFileRecordExpiredTimeUs())
            .Times(1)
            .WillOnce(Return(1ul));

        // test client version valid
        FileInfo snapShotFileInfoRet;
        ASSERT_EQ(curvefs_->CreateSnapShotFile("/originalFile2",
                &snapShotFileInfoRet), StatusCode::kOK);
        ASSERT_EQ(snapShotFileInfoRet.parentid(), originalFile.id());
        ASSERT_EQ(snapShotFileInfoRet.filename(),
            originalFile.filename() + "-" +
            std::to_string(originalFile.seqnum()) );
        ASSERT_EQ(snapShotFileInfoRet.filestatus(), FileStatus::kFileCreated);
        ASSERT_EQ(
            snapShotFileInfoRet.filetype(), FileType::INODE_SNAPSHOT_PAGEFILE);
    }
    {
        // test storage snapshotFile Error
    }
}

TEST_F(CurveFSTest, testListSnapShotFile) {
    {
        // workPath error
    }
    {
        // dir not support
        std::vector<FileInfo> snapFileInfos;
        ASSERT_EQ(curvefs_->ListSnapShotFile("/", &snapFileInfos),
        StatusCode::kNotSupported);
    }
    {
        // lookupFile error
        std::vector<FileInfo> snapFileInfos;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->ListSnapShotFile("/originalFile", &snapFileInfos),
            StatusCode::kFileNotExists);
    }
    {
        // check type not support
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_DIRECTORY);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapFileInfos;
        ASSERT_EQ(curvefs_->ListSnapShotFile("originalFile", &snapFileInfos),
        StatusCode::kNotSupported);
    }
    {
        // ListFile error
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        std::vector<FileInfo> snapFileInfos;
        ASSERT_EQ(curvefs_->ListSnapShotFile("originalFile", &snapFileInfos),
        StatusCode::kStorageError);
    }
    {
        // ListFile ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapFileInfos;
        FileInfo  snapShotFile;
        snapShotFile.set_parentid(1);
        snapFileInfos.push_back(snapShotFile);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapFileInfos),
        Return(StoreStatus::OK)));

        std::vector<FileInfo> snapFileInfosRet;
        ASSERT_EQ(curvefs_->ListSnapShotFile("originalFile", &snapFileInfosRet),
        StatusCode::kOK);

        ASSERT_EQ(snapFileInfosRet.size(), 1);
        ASSERT_EQ(snapFileInfosRet[0].SerializeAsString(),
                snapShotFile.SerializeAsString());
        }
}


TEST_F(CurveFSTest, testGetSnapShotFileInfo) {
    {
        // ListSnapShotFile error
        FileInfo snapshotFileInfo;
        ASSERT_EQ(curvefs_->GetSnapShotFileInfo("/", 1, &snapshotFileInfo),
        StatusCode::kNotSupported);
    }
    {
        // snapfile not exist(not under snapshot)
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        FileInfo snapshotFileInfo;
        ASSERT_EQ(curvefs_->GetSnapShotFileInfo("/originalFile",
            1, &snapshotFileInfo), StatusCode::kSnapshotFileNotExists);
    }
    {
        // under snapshot, butsnapfile not exist
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(2);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        FileInfo snapshotFileInfo;
        ASSERT_EQ(curvefs_->GetSnapShotFileInfo("/originalFile",
            1, &snapshotFileInfo), StatusCode::kSnapshotFileNotExists);
    }
    {
        // test ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        FileInfo snapshotFileInfo;
        ASSERT_EQ(curvefs_->GetSnapShotFileInfo("/originalFile",
            1, &snapshotFileInfo), StatusCode::kOK);
        ASSERT_EQ(snapshotFileInfo.SerializeAsString(),
        snapInfo.SerializeAsString());
    }
}

TEST_F(CurveFSTest, GetSnapShotFileSegment) {
    {
        // GetSnapShotFileInfo error
        PageFileSegment segment;
        ASSERT_EQ(curvefs_->GetSnapShotFileSegment("/", 1, 0, &segment),
            StatusCode::kNotSupported);
    }
    {
        // offset not align
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_segmentsize(DefaultSegmentSize);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        PageFileSegment segment;
        ASSERT_EQ(curvefs_->GetSnapShotFileSegment("/originalFile",
            1, 1, &segment), StatusCode::kParaError);
    }
    {
        // storage->GetSegment return error
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_segmentsize(DefaultSegmentSize);
        originalFile.set_length(DefaultSegmentSize);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_segmentsize(DefaultSegmentSize);
        snapInfo.set_length(DefaultSegmentSize);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        PageFileSegment segment;
        ASSERT_EQ(curvefs_->GetSnapShotFileSegment("/originalFile",
            1, 0, &segment), StatusCode::kSegmentNotAllocated);
    }
    {
        // ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_segmentsize(DefaultSegmentSize);
        originalFile.set_length(DefaultSegmentSize);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));


        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_segmentsize(DefaultSegmentSize);
        snapInfo.set_length(DefaultSegmentSize);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        PageFileSegment expectSegment;
        expectSegment.set_logicalpoolid(1);
        expectSegment.set_segmentsize(DefaultSegmentSize);
        expectSegment.set_chunksize(curvefs_->GetDefaultChunkSize());
        expectSegment.set_startoffset(0);

        PageFileChunkInfo *chunkInfo = expectSegment.add_chunks();
        chunkInfo->set_chunkid(1);
        chunkInfo->set_copysetid(1);

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(expectSegment),
            Return(StoreStatus::OK)));

        PageFileSegment segment;
        ASSERT_EQ(curvefs_->GetSnapShotFileSegment("/originalFile",
            1, 0, &segment), StatusCode::kOK);
        ASSERT_EQ(expectSegment.SerializeAsString(),
                    segment.SerializeAsString());
    }
}

TEST_F(CurveFSTest, DeleteFileSnapShotFile) {
    {
        // GetSnapShotFileInfo error
        FileInfo snapshotFileInfo;
        ASSERT_EQ(curvefs_->DeleteFileSnapShotFile("/", 1, nullptr),
        StatusCode::kNotSupported);
    }
    {
        // under deleteing
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_EQ(curvefs_->DeleteFileSnapShotFile("/originalFile", 1, nullptr),
            StatusCode::kSnapshotDeleting);
    }
    {
        // delete snapshot file filetype error (internal case)
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_filename("originalFile-seq1");
        snapInfo.set_seqnum(1);
        snapInfo.set_filetype(FileType::INODE_APPENDFILE);
        snapShotFiles.push_back(snapInfo);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_EQ(curvefs_->DeleteFileSnapShotFile("/originalFile", 1, nullptr),
            StatusCode::KInternalError);
    }
    {
        // delete storage error
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_filename("originalFile-seq1");
        snapInfo.set_seqnum(1);
        snapInfo.set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
        snapInfo.set_filestatus(FileStatus::kFileCreated);
        snapShotFiles.push_back(snapInfo);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_EQ(curvefs_->DeleteFileSnapShotFile("/originalFile", 1, nullptr),
            StatusCode::KInternalError);
    }
    {
        // delete snapshot ok
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_filename("originalFile-seq1");
        snapInfo.set_seqnum(1);
        snapInfo.set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
        snapInfo.set_filestatus(FileStatus::kFileCreated);
        snapShotFiles.push_back(snapInfo);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*mockcleanManager_,
            SubmitDeleteSnapShotFileJob(_, _))
        .Times(1)
        .WillOnce(Return(true));

        EXPECT_EQ(curvefs_->DeleteFileSnapShotFile("/originalFile", 1, nullptr),
            StatusCode::kOK);
    }
    {
        //  message the snapshot delete manager error, return error
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_filename("originalFile-seq1");
        snapInfo.set_seqnum(1);
        snapInfo.set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
        snapInfo.set_filestatus(FileStatus::kFileCreated);
        snapShotFiles.push_back(snapInfo);

        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*mockcleanManager_,
            SubmitDeleteSnapShotFileJob(_, _))
        .Times(1)
        .WillOnce(Return(false));

        EXPECT_EQ(curvefs_->DeleteFileSnapShotFile("/originalFile", 1, nullptr),
            StatusCode::KInternalError);
    }
}

TEST_F(CurveFSTest, CheckSnapShotFileStatus) {
    // GetSnapShotFileInfo error
    {
        PageFileSegment segment;
        ASSERT_EQ(curvefs_->GetSnapShotFileSegment("/", 1, 0, &segment),
            StatusCode::kNotSupported);
    }

    // snapshot file is not deleting
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileCreated);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kOK);
        ASSERT_EQ(fileStatus, FileStatus::kFileCreated);
        ASSERT_EQ(progress, 0);
    }

    // snapshot file is deleting, task is not exist, delete success
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);

        std::vector<FileInfo> snapShotFiles2;
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles2),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*mockcleanManager_,
            GetTask(_))
        .Times(1)
        .WillOnce(Return(nullptr));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kSnapshotFileNotExists);
        ASSERT_EQ(progress, 100);
    }

    // snapshot file is deleting, task is not exist, delete failed
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*mockcleanManager_,
            GetTask(_))
        .Times(1)
        .WillOnce(Return(nullptr));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kSnapshotFileDeleteError);
        ASSERT_EQ(fileStatus, FileStatus::kFileDeleting);
        ASSERT_EQ(progress, 0);
    }

    // snapshot file is deleting, task is PROGRESSING
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        auto task =
                 std::make_shared<SnapShotCleanTask>(1, nullptr, originalFile);
        TaskProgress taskProgress;
        taskProgress.SetProgress(50);
        taskProgress.SetStatus(TaskStatus::PROGRESSING);
        task->SetTaskProgress(taskProgress);
        EXPECT_CALL(*mockcleanManager_, GetTask(_))
        .Times(1)
        .WillOnce(Return(task));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kOK);
        ASSERT_EQ(fileStatus, FileStatus::kFileDeleting);
        ASSERT_EQ(progress, 50);
    }

    // snapshot file is deleting, task is FAILED
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        auto task =
        std::make_shared<SnapShotCleanTask>(1, nullptr, originalFile);
        TaskProgress taskProgress;
        taskProgress.SetProgress(50);
        taskProgress.SetStatus(TaskStatus::FAILED);
        task->SetTaskProgress(taskProgress);
        EXPECT_CALL(*mockcleanManager_, GetTask(_))
        .Times(1)
        .WillOnce(Return(task));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kOK);
        ASSERT_EQ(fileStatus, FileStatus::kFileDeleting);
        ASSERT_EQ(progress, 50);
    }

    // snapshot file is deleting, task is SUCCESS
    {
        FileInfo originalFile;
        originalFile.set_id(1);
        originalFile.set_seqnum(1);
        originalFile.set_filename("originalFile");
        originalFile.set_filetype(FileType::INODE_PAGEFILE);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(originalFile),
            Return(StoreStatus::OK)));

        std::vector<FileInfo> snapShotFiles;
        FileInfo snapInfo;
        snapInfo.set_seqnum(1);
        snapInfo.set_filestatus(FileStatus::kFileDeleting);
        snapShotFiles.push_back(snapInfo);
        EXPECT_CALL(*storage_, ListSnapshotFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(snapShotFiles),
                Return(StoreStatus::OK)));

        auto task =
        std::make_shared<SnapShotCleanTask>(1, nullptr, originalFile);
        TaskProgress taskProgress;
        taskProgress.SetProgress(100);
        taskProgress.SetStatus(TaskStatus::SUCCESS);
        task->SetTaskProgress(taskProgress);
        EXPECT_CALL(*mockcleanManager_, GetTask(_))
        .Times(1)
        .WillOnce(Return(task));

        FileStatus fileStatus;
        uint32_t progress;
        ASSERT_EQ(curvefs_->CheckSnapShotFileStatus("/originalFile",
            1, &fileStatus, &progress), StatusCode::kOK);
        ASSERT_EQ(fileStatus, FileStatus::kFileDeleting);
        ASSERT_EQ(progress, 100);
    }
}

TEST_F(CurveFSTest, testOpenFile) {
    // 文件不存在
    {
        ProtoSession protoSession;
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1",
                                     &protoSession, &fileInfo),
                  StatusCode::kFileNotExists);
        ASSERT_EQ(curvefs_->GetOpenFileNum(), 0);
    }

    // open目录
    {
        ProtoSession protoSession;
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1",
                                     &protoSession, &fileInfo),
                  StatusCode::kNotSupported);
        ASSERT_EQ(curvefs_->GetOpenFileNum(), 0);
    }

    // 执行成功
    {
        ProtoSession protoSession;
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(
            curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession, &fileInfo),
            StatusCode::kOK);
    }

    // open clone file, clone source is not a valid curve file
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("123456-7890-xxxx-xxxxxx");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::OK));

        CloneSourceSegment sourceSegment;
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment),
                  StatusCode::kOK);
        ASSERT_FALSE(sourceSegment.IsInitialized());
    }

    // open a flattened clone file
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloned);
        CloneSourceSegment sourceSegment;

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(StatusCode::kOK,
                  curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment));
        ASSERT_FALSE(sourceSegment.IsInitialized());
    }

    // open clone file, cloneSourceSegments is nullptr
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, nullptr),
                  StatusCode::kParaError);
    }

    // open clone file, get clone file info failed
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::OK))
            .WillOnce(Return(StoreStatus::KeyNotExist));

        CloneSourceSegment sourceSegment;
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment),
                  StatusCode::kFileNotExists);
        ASSERT_FALSE(sourceSegment.IsInitialized());
    }

    // open clone file, list clone file segment failed
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(2)
            .WillRepeatedly(Return(StoreStatus::OK));

        EXPECT_CALL(*storage_, ListSegment(_, _))
            .WillOnce(Return(StoreStatus::InternalError));

        CloneSourceSegment sourceSegment;
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment),
                  StatusCode::kStorageError);
        ASSERT_FALSE(sourceSegment.IsInitialized());
    }

    // open clone file, clone source file has no segments
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(2)
            .WillRepeatedly(Return(StoreStatus::OK));

        std::vector<PageFileSegment> segments;
        EXPECT_CALL(*storage_, ListSegment(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(segments), Return(StoreStatus::OK)));

        CloneSourceSegment sourceSegment;
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment),
                  StatusCode::kOK);

        ASSERT_TRUE(sourceSegment.IsInitialized());
        ASSERT_EQ(0, sourceSegment.allocatedsegmentoffset_size());
    }

    // open clone file success
    {
        ProtoSession protoSession;
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_clonesource("/clonefile");
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);
        fileInfo.set_segmentsize(1 * kGB);

        FileInfo cloneSourceFileInfo;
        cloneSourceFileInfo.set_segmentsize(1 * kGB);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::OK))
            .WillOnce(DoAll(SetArgPointee<2>(cloneSourceFileInfo),
                            Return(StoreStatus::OK)));

        PageFileSegment segment1;
        segment1.set_logicalpoolid(1);
        segment1.set_segmentsize(1 * kGB);
        segment1.set_chunksize(16 * kMB);
        segment1.set_startoffset(0 * kGB);

        PageFileSegment segment2;
        segment2.set_logicalpoolid(1);
        segment2.set_segmentsize(1 * kGB);
        segment2.set_chunksize(16 * kMB);
        segment2.set_startoffset(1 * kGB);

        std::vector<PageFileSegment> segments{segment1, segment2};

        EXPECT_CALL(*storage_, ListSegment(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(segments), Return(StoreStatus::OK)));

        CloneSourceSegment sourceSegment;
        ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession,
                                     &fileInfo, &sourceSegment),
                  StatusCode::kOK);

        ASSERT_TRUE(sourceSegment.IsInitialized());
        ASSERT_EQ(2, sourceSegment.allocatedsegmentoffset_size());
        ASSERT_EQ(0 * kGB, sourceSegment.allocatedsegmentoffset(0));
        ASSERT_EQ(1 * kGB, sourceSegment.allocatedsegmentoffset(1));
        ASSERT_EQ(1 * kGB, sourceSegment.segmentsize());
    }
}

TEST_F(CurveFSTest, testCloseFile) {
    ProtoSession protoSession;
    FileInfo  fileInfo;
    fileInfo.set_filetype(FileType::INODE_PAGEFILE);

    // 先插入session
    EXPECT_CALL(*storage_, GetFile(_, _, _))
    .Times(1)
    .WillOnce(Return(StoreStatus::OK));

    ASSERT_EQ(
        curvefs_->OpenFile("/file1", "127.0.0.1", &protoSession, &fileInfo),
        StatusCode::kOK);

    // 执行成功
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->CloseFile("/file1", protoSession.sessionid()),
                  StatusCode::kOK);
    }
}

TEST_F(CurveFSTest, testRefreshSession) {
    ProtoSession protoSession;
    FileInfo  fileInfo;
    fileInfo.set_filetype(FileType::INODE_PAGEFILE);

    // 先插入session
    EXPECT_CALL(*storage_, GetFile(_, _, _))
    .Times(1)
    .WillOnce(Return(StoreStatus::OK));

    ASSERT_EQ(curvefs_->OpenFile("/file1", "127.0.0.1",
                                    &protoSession, &fileInfo),
                StatusCode::kOK);

    // 文件不存在
    {
        FileInfo  fileInfo1;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(curvefs_->RefreshSession("/file1", "sessionidxxxxx", 12345,
                    "signaturexxxx", "127.0.0.1", 1234, "", &fileInfo1),
                  StatusCode::kFileNotExists);
    }

    // 执行成功
    {
        FileInfo  fileInfo1;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        uint64_t date = ::curve::common::TimeUtility::GetTimeofDayUs();
        ASSERT_EQ(curvefs_->RefreshSession("/file1", protoSession.sessionid(),
                    date, "signaturexxxx", "127.0.0.1", 1234, "", &fileInfo1),
                  StatusCode::kOK);
        ASSERT_EQ(1, curvefs_->GetOpenFileNum());
    }
}

TEST_F(CurveFSTest, testCheckRenameNewfilePathOwner) {
    uint64_t date = TimeUtility::GetTimeofDayUs();

    // root用户，签名匹配，date超时
    {
        std::string filename = "/file1";
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                authOptions_.rootOwner);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                authOptions_.rootPassword);
        ASSERT_EQ(curvefs_->CheckDestinationOwner(filename,
                    authOptions_.rootOwner, sig, date),
                  StatusCode::kOK);

        ASSERT_EQ(curvefs_->CheckDestinationOwner(filename,
                    authOptions_.rootOwner, sig, date + 15 * 2000 * 2000),
                  StatusCode::kOwnerAuthFail);
    }

    // root用户，签名不匹配
    {
        ASSERT_EQ(curvefs_->CheckDestinationOwner("/file1",
                    authOptions_.rootOwner, "wrongpass", date),
                  StatusCode::kOwnerAuthFail);
    }

    // 普通用户，根目录下的文件非root用户认证失败
    {
        FileInfo fileInfo;
        fileInfo.set_owner(authOptions_.rootOwner);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                Return(StoreStatus::OK)));
        ASSERT_EQ(curvefs_->CheckDestinationOwner("/file1",
                    "normaluser", "wrongpass", date),
                  StatusCode::kOwnerAuthFail);
    }
}

TEST_F(CurveFSTest, testCheckPathOwner) {
    uint64_t date = TimeUtility::GetTimeofDayUs();

    // root用户，签名匹配, 并检测date过期
    {
        std::string filename = "/file1";
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                authOptions_.rootOwner);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                authOptions_.rootPassword);

        ASSERT_EQ(curvefs_->CheckPathOwner(filename,
                    authOptions_.rootOwner, sig, date),
                  StatusCode::kOK);

        ASSERT_EQ(curvefs_->CheckPathOwner(filename, authOptions_.rootOwner,
                                           sig, date + 15 * 2000 * 2000),
                  StatusCode::kOwnerAuthFail);
    }

    // root用户，签名不匹配
    {
        ASSERT_EQ(curvefs_->CheckPathOwner("/file1", authOptions_.rootOwner,
                                            "wrongpass", date),
                  StatusCode::kOwnerAuthFail);
    }

    // 普通用户，根目录下的文件非root用户认证成功, 并检测date超时
    {
        ASSERT_EQ(curvefs_->CheckPathOwner("/file1", "normaluser",
                                            "wrongpass", date),
                  StatusCode::kOK);

        ASSERT_EQ(curvefs_->CheckPathOwner("/file1", "normaluser", "wrongpass",
                                            date + 15 * 2000 * 2000),
                  StatusCode::kOwnerAuthFail);
    }
}

TEST_F(CurveFSTest, testCheckFileOwner) {
    uint64_t date = TimeUtility::GetTimeofDayUs();
    // root用户，签名匹配
    {
        std::string filename = "/file1";
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                authOptions_.rootOwner);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                authOptions_.rootPassword);

        ASSERT_EQ(curvefs_->CheckFileOwner(filename,
                    authOptions_.rootOwner, sig, date),
                  StatusCode::kOK);
        ASSERT_EQ(curvefs_->CheckFileOwner(filename,
                    authOptions_.rootOwner, sig, date + 15 * 2000 * 2000),
                  StatusCode::kOwnerAuthFail);
    }

    // root用户，签名不匹配
    {
        ASSERT_EQ(curvefs_->CheckFileOwner("/file1",
                    authOptions_.rootOwner, "wrongpass", date),
                  StatusCode::kOwnerAuthFail);
    }

    // 普通用户，根目录下的文件非root用户认证成功
    {
        FileInfo fileInfo;
        fileInfo.set_owner("normaluser");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->CheckFileOwner("/file1",
                    "normaluser", "", date), StatusCode::kOK);
    }

    // 普通用户，根目录下的文件非root用户认证失败
    {
        FileInfo fileInfo;
        fileInfo.set_owner("normaluser");
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->CheckFileOwner("/file1",
                    "normaluser1", "", date), StatusCode::kOwnerAuthFail);
    }
}


TEST_F(CurveFSTest, testCreateCloneFile) {
    // test parm error
    ASSERT_EQ(curvefs_->CreateCloneFile("/file1", "owner1",
                FileType::INODE_DIRECTORY, kMiniFileLength, kStartSeqNum,
                curvefs_->GetDefaultChunkSize(), nullptr),
                StatusCode::kParaError);

    ASSERT_EQ(curvefs_->CreateCloneFile("/file1", "owner1",
                FileType::INODE_PAGEFILE, kMiniFileLength - 1, kStartSeqNum,
                curvefs_->GetDefaultChunkSize(), nullptr),
                StatusCode::kParaError);

    {
        // test file exist
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        auto statusCode = curvefs_->CreateCloneFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, kStartSeqNum,
                    curvefs_->GetDefaultChunkSize(), nullptr);
        ASSERT_EQ(statusCode, StatusCode::kFileExists);
    }

    {
        // test get storage error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        auto statusCode = curvefs_->CreateCloneFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, kStartSeqNum,
                    curvefs_->GetDefaultChunkSize(), nullptr);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test inode allocate error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(false));

        auto statusCode = curvefs_->CreateCloneFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, kStartSeqNum,
                    curvefs_->GetDefaultChunkSize(), nullptr);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test put storage error
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));

        auto statusCode = curvefs_->CreateCloneFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, kStartSeqNum,
                    curvefs_->GetDefaultChunkSize(), nullptr);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }
    {
        // test ok
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        FileInfo fileInfo;
        auto statusCode = curvefs_->CreateCloneFile("/file1", "owner1",
                    FileType::INODE_PAGEFILE, kMiniFileLength, kStartSeqNum,
                    curvefs_->GetDefaultChunkSize(), &fileInfo);
        ASSERT_EQ(statusCode, StatusCode::kOK);
        ASSERT_EQ(fileInfo.filename(), "file1");
        ASSERT_EQ(fileInfo.owner(), "owner1");
        ASSERT_EQ(fileInfo.filetype(), FileType::INODE_PAGEFILE);
        ASSERT_EQ(fileInfo.filestatus(), FileStatus::kFileCloning);
        ASSERT_EQ(fileInfo.length(), kMiniFileLength);
        ASSERT_EQ(fileInfo.segmentsize(), DefaultSegmentSize);
        ASSERT_EQ(fileInfo.chunksize(), curvefs_->GetDefaultChunkSize());
        ASSERT_EQ(fileInfo.seqnum(), kStartSeqNum);
    }
}

TEST_F(CurveFSTest, testSetCloneFileStatus) {
    {
        // test path not exist
        FileInfo  fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1/file2",
            kUnitializedFileID, FileStatus::kFileCloned),
                  StatusCode::kFileNotExists);
    }

    {
        // test stoarge error
        FileInfo fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1/file2",
            kUnitializedFileID, FileStatus::kFileCloned),
                StatusCode::kStorageError);
    }

    {
        // test  WalkPath NOT DIRECTORY
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1/file2",
            kUnitializedFileID, FileStatus::kFileCloned),
                StatusCode::kFileNotExists);
    }
    {
        // test LookUpFile internal Error
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1/file2",
            kUnitializedFileID, FileStatus::kFileCloned),
                StatusCode::kStorageError);
    }
    {
        // test inodeid not match
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_id(100);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1",
            10, FileStatus::kFileCloned),
                StatusCode::kFileIdNotMatch);
    }
    {
        // test filestatus not ok
        const struct {
            FileStatus originStatus;
            FileStatus setStatus;
            StatusCode expectReturn;
            int putFileTime;
        } testCases[] {
            {FileStatus::kFileCloning, FileStatus::kFileCloneMetaInstalled,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloning, FileStatus::kFileCloning,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloneMetaInstalled, FileStatus::kFileCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloneMetaInstalled,
                FileStatus::kFileCloneMetaInstalled,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloned, FileStatus::kFileCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileCreated, FileStatus::kFileBeingCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileBeingCloned, FileStatus::kFileCreated,
                StatusCode::kOK, 1},
            {FileStatus::kFileBeingCloned, FileStatus::kFileBeingCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloned, FileStatus::kFileBeingCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileBeingCloned, FileStatus::kFileCloned,
                StatusCode::kOK, 1},
            {FileStatus::kFileCreated, FileStatus::kFileCreated,
                StatusCode::kOK, 1},
            {FileStatus::kFileCloning, FileStatus::kFileCloned,
                StatusCode::kCloneStatusNotMatch, 0},
            {FileStatus::kFileCloneMetaInstalled, FileStatus::kFileCloning,
                StatusCode::kCloneStatusNotMatch, 0},
            {FileStatus::kFileCreated, FileStatus::kFileCloned,
                StatusCode::kCloneStatusNotMatch, 0},
            {FileStatus::kFileDeleting, FileStatus::kFileBeingCloned,
                StatusCode::kCloneStatusNotMatch, 0},
            {FileStatus::kFileCloning, FileStatus::kFileBeingCloned,
                StatusCode::kCloneStatusNotMatch, 0},
            {FileStatus::kFileCloneMetaInstalled, FileStatus::kFileBeingCloned,
                StatusCode::kCloneStatusNotMatch, 0}
        };

        for (int i = 0; i < sizeof(testCases) / sizeof(testCases[0]); i++) {
            {
                FileInfo fileInfo;
                fileInfo.set_filetype(FileType::INODE_PAGEFILE);
                fileInfo.set_filestatus(testCases[i].originStatus);
                EXPECT_CALL(*storage_, GetFile(_, _, _))
                .Times(1)
                .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                    Return(StoreStatus::OK)));

                EXPECT_CALL(*storage_, PutFile(_))
                .Times(AtLeast(testCases[i].putFileTime))
                .WillOnce(Return(StoreStatus::OK));


                ASSERT_EQ(curvefs_->SetCloneFileStatus("/dir1",
                    kUnitializedFileID, testCases[i].setStatus),
                    testCases[i].expectReturn);
            }
        }
    }
    {
        // test put file error
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/file1",
            kUnitializedFileID, FileStatus::kFileCloned),
                StatusCode::kStorageError);
    }
    {
        // test put file ok
        FileInfo fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo.set_filestatus(FileStatus::kFileCloneMetaInstalled);
        EXPECT_CALL(*storage_, GetFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->SetCloneFileStatus("/file1",
            kUnitializedFileID, FileStatus::kFileCloned),
                StatusCode::kOK);
    }
}

TEST_F(CurveFSTest, Init) {
    // test getFile ok
    {
        FileInfo fileInfo1, fileInfo2, fileInfo3, fileInfo4, fileInfo5;
        fileInfo1.set_parentid(ROOTINODEID+1);

        fileInfo2.set_parentid(ROOTINODEID);
        fileInfo2.set_id(RECYCLEBININODEID+1);

        fileInfo3.set_parentid(ROOTINODEID);
        fileInfo3.set_id(RECYCLEBININODEID);
        fileInfo3.set_filename(RECYCLEBINDIRNAME + "aa");

        fileInfo4.set_parentid(ROOTINODEID);
        fileInfo4.set_id(RECYCLEBININODEID);
        fileInfo4.set_filename(RECYCLEBINDIRNAME);
        fileInfo4.set_filetype(FileType::INODE_PAGEFILE);

        fileInfo4.set_parentid(ROOTINODEID);
        fileInfo4.set_id(RECYCLEBININODEID);
        fileInfo4.set_filename(RECYCLEBINDIRNAME);
        fileInfo4.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo4.set_owner("imanotroot");

        fileInfo5.set_parentid(ROOTINODEID);
        fileInfo5.set_id(RECYCLEBININODEID);
        fileInfo5.set_filename(RECYCLEBINDIRNAME);
        fileInfo5.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo5.set_owner(authOptions_.rootOwner);

        const struct {
            FileInfo info;
            bool      ret;
        } testCases[] = {
            {fileInfo1, false},
            {fileInfo2, false},
            {fileInfo3, false},
            {fileInfo4, false},
            {fileInfo5, true},
        };

        for (int i = 0; i < sizeof(testCases)/ sizeof(testCases[0]); i++) {
            EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(testCases[i].info),
                Return(StoreStatus::OK)));

            ASSERT_EQ(testCases[i].ret, kCurveFS.Init(storage_,
                                                      inodeIdGenerator_,
                                                      mockChunkAllocator_,
                                                      mockcleanManager_,
                                                      fileRecordManager_,
                                                      allocStatistic_,
                                                      curveFSOptions_,
                                                      topology_,
                                                      nullptr));
        }
    }

    // test internal error
    {
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(false, kCurveFS.Init(storage_,
                                       inodeIdGenerator_,
                                       mockChunkAllocator_,
                                       mockcleanManager_,
                                       fileRecordManager_,
                                       allocStatistic_,
                                       curveFSOptions_,
                                       topology_,
                                       nullptr));
    }

    // test getfile not exist
    {
        // putfile error case
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
            .Times(1)
            .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(false, kCurveFS.Init(storage_,
                                       inodeIdGenerator_,
                                       mockChunkAllocator_,
                                       mockcleanManager_,
                                       fileRecordManager_,
                                       allocStatistic_,
                                       curveFSOptions_,
                                       topology_,
                                       nullptr));

        // putfile ok
        FileInfo fileInfo5;
        fileInfo5.set_parentid(ROOTINODEID);
        fileInfo5.set_id(RECYCLEBININODEID);
        fileInfo5.set_filename(RECYCLEBINDIRNAME);
        fileInfo5.set_filetype(FileType::INODE_DIRECTORY);
        fileInfo5.set_owner(ROOTUSERNAME);

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .Times(1)
            .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_))
            .Times(1)
            .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(true, kCurveFS.Init(storage_,
                                      inodeIdGenerator_,
                                      mockChunkAllocator_,
                                      mockcleanManager_,
                                      fileRecordManager_,
                                      allocStatistic_,
                                      curveFSOptions_,
                                      topology_,
                                      nullptr));
    }
}

TEST_F(CurveFSTest, ListClient) {
    std::vector<ClientInfo> clientInfos;
    ASSERT_EQ(StatusCode::kOK, curvefs_->ListClient(false, &clientInfos));
    ASSERT_EQ(0, clientInfos.size());

    fileRecordManager_->UpdateFileRecord("/file1", "0.0.6", "127.0.0.1", 1234);
    fileRecordManager_->UpdateFileRecord("/file2", "0.0.6", "127.0.0.1", 1234);
    ASSERT_EQ(StatusCode::kOK, curvefs_->ListClient(false, &clientInfos));
    ASSERT_EQ(1, clientInfos.size());

    clientInfos.clear();
    fileRecordManager_->UpdateFileRecord("/file3", "0.0.6", "127.0.0.1", 1235);
    ASSERT_EQ(StatusCode::kOK, curvefs_->ListClient(false, &clientInfos));
    ASSERT_EQ(2, clientInfos.size());
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);

    return RUN_ALL_TESTS();
}

}  // namespace mds
}  // namespace curve
