/*
 * Project: curve
 * Created Date: Wednesday September 12th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "test/mds/nameserver2/mock_namespace_storage.h"
#include "test/mds/nameserver2/mock_inode_id_generator.h"
#include "test/mds/nameserver2/mock_chunk_allocate.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;

namespace curve {
namespace mds {

class CurveFSTest: public ::testing::Test {
 protected:
    void SetUp() override {
        storage_ = new MockNameServerStorage();
        inodeIdGenerator_ = new MockInodeIDGenerator();
        mockChunkAllocator_ = new MockChunkAllocator();
        curvefs_ =  &kCurveFS;
        curvefs_->Init(storage_, inodeIdGenerator_, mockChunkAllocator_);
    }

    void TearDown() override {
        delete storage_;
        delete inodeIdGenerator_;
        delete mockChunkAllocator_;
    }

    CurveFS *curvefs_;
    MockNameServerStorage *storage_;
    MockInodeIDGenerator *inodeIdGenerator_;
    MockChunkAllocator *mockChunkAllocator_;
};

TEST_F(CurveFSTest, testCreateFile1) {
    // test parm error
    ASSERT_EQ(curvefs_->CreateFile("/file1", FileType::INODE_PAGEFILE,
                                   kMiniFileLength-1), StatusCode::kParaError);

    ASSERT_EQ(curvefs_->CreateFile("/", FileType::INODE_DIRECTORY, 0),
              StatusCode::kFileExists);

    {
        // test file exist
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        auto statusCode = curvefs_->CreateFile("/file1",
                    FileType::INODE_PAGEFILE, kMiniFileLength);
        ASSERT_EQ(statusCode, StatusCode::kFileExists);
    }

    {
        // test get storage error
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        auto statusCode = curvefs_->CreateFile("/file1",
                    FileType::INODE_PAGEFILE, kMiniFileLength);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test put storage error
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));

        auto statusCode = curvefs_->CreateFile("/file1",
                    FileType::INODE_PAGEFILE, kMiniFileLength);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
    }

    {
        // test put storage ok
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, PutFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(true));


        auto statusCode = curvefs_->CreateFile("/file1",
            FileType::INODE_PAGEFILE, kMiniFileLength);
        ASSERT_EQ(statusCode, StatusCode::kOK);
    }

    {
        // test inode allocate error
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*inodeIdGenerator_, GenInodeID(_))
        .Times(1)
        .WillOnce(Return(false));

        auto statusCode = curvefs_->CreateFile("/file1",
                FileType::INODE_PAGEFILE, kMiniFileLength);
        ASSERT_EQ(statusCode, StatusCode::kStorageError);
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
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));
        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kFileNotExists);
    }
    {
        // test stoarge error
        FileInfo fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));
        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kStorageError);
    }
    {
        // test  ok
        FileInfo fileInfo;
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillRepeatedly(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->GetFileInfo("/file1/file2", &fileInfo),
                  StatusCode::kOK);
    }
    {
        // test  WalkPath NOT DIRECTORY
        FileInfo  fileInfo;
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
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
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
            Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo fileInfo1;
        ASSERT_EQ(curvefs_->GetFileInfo("testdir/file1", &fileInfo1),
            StatusCode::kStorageError);
    }
}


TEST_F(CurveFSTest, testDeleteFile) {
    // test remove root
    ASSERT_EQ(curvefs_->DeleteFile("/"), StatusCode::kParaError);

    // test ok
    {
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(StoreStatus::OK));

        EXPECT_CALL(*storage_, DeleteFile(_))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteFile("/file1"), StatusCode::kOK);
    }

    //  test filenotexist fail
    {
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->DeleteFile("/file1"), StatusCode::kFileNotExists);
    }

    // test storage delete  error
    {
        EXPECT_CALL(*storage_, DeleteFile(_))
        .Times(AtLeast(1))
        .WillOnce(Return(StoreStatus::InternalError));

        ASSERT_EQ(curvefs_->DeleteFile("/file1"), StatusCode::kStorageError);
    }
}

TEST_F(CurveFSTest, testReadDir) {
    FileInfo fileInfo;
    std::vector<FileInfo> items;

    // test not directory
    {
        fileInfo.set_filetype(FileType::INODE_PAGEFILE);
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ReadDir("/file1", &items),
                  StatusCode::kNotDirectory);
        items.clear();
    }

    // test getFile Not exist
    {
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));

        ASSERT_EQ(curvefs_->ReadDir("/file1", &items),
                  StatusCode::kDirNotExist);
    }

    // test listFile ok
    {
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
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
    FileInfo fileInfo;

    // test rename ok
    {
        fileInfo.set_filetype(FileType::INODE_DIRECTORY);
        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(AtLeast(3))
        .WillOnce(Return(StoreStatus::OK))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(StoreStatus::OK)))
        .WillOnce(Return(StoreStatus::KeyNotExist));

        EXPECT_CALL(*storage_, RenameFile(_, _, _, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->RenameFile("/file1", "/trash/file2"),
                  StatusCode::kOK);
    }

    // TODO(hzsunjianliang): add abnormal cases here
}

TEST_F(CurveFSTest, testExtentFile) {
    // test try small filesize && same
    {
        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);


        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtentFile("/user1/file1", 0),
                  StatusCode::kShrinkBiggerFile);

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtentFile("/user1/file1",
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

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        ASSERT_EQ(curvefs_->ExtentFile("/user1/file1",
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

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, PutFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->ExtentFile("/user1/file1",
                                       2 * kMiniFileLength), StatusCode::kOK);
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

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _))
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

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::KeyNotExist));


        EXPECT_CALL(*mockChunkAllocator_, AllocateChunkSegment(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(true));


        EXPECT_CALL(*storage_, PutSegment(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->GetOrAllocateSegment("/user1/file2",
                  0, true,  &segment), StatusCode::kOK);
    }
    // TODO(hzsunjianliang): add abnormal case
}


TEST_F(CurveFSTest, testDeleteSegment) {
    // test normal delete exist segment
    {
        PageFileSegment segment;

        FileInfo fileInfo1;
        fileInfo1.set_filetype(FileType::INODE_DIRECTORY);

        FileInfo fileInfo2;
        fileInfo2.set_filetype(FileType::INODE_PAGEFILE);
        fileInfo2.set_length(kMiniFileLength);
        fileInfo2.set_segmentsize(DefaultSegmentSize);

        EXPECT_CALL(*storage_, GetFile(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo1),
                        Return(StoreStatus::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo2),
                        Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSegment(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*storage_, DeleteSegment(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        ASSERT_EQ(curvefs_->DeleteSegment("/user1/file2",
                                          0), StatusCode::kOK);
    }
    // TODO(hzsunjianliang): add abnormal case
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);

    return RUN_ALL_TESTS();
}

}  // namespace mds
}  // namespace curve
