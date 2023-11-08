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
 * Created Date: 2023-11-02
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/volume/volume_service_manager.h"

#include "test/snapshotcloneserver/mock_snapshot_server.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Property;
using curve::client::FInfo_t;

namespace curve {
namespace snapshotcloneserver {

class TestVolumeServiceManager : public ::testing::Test {
 public:
    void SetUp() override {
        client_ = std::make_shared<MockCurveFsClient>();
        volumeServiceManager_ = std::make_shared<VolumeServiceManager>(client_);
    }

    void TearDown() override {
        volumeServiceManager_ = nullptr;
    }

 protected:
    std::shared_ptr<MockCurveFsClient> client_;
    std::shared_ptr<VolumeServiceManager> volumeServiceManager_;
};

TEST_F(TestVolumeServiceManager, TestCreateFile) {
    std::string fileName = "test";
    std::string owner = "test";
    uint64_t size = 0;
    uint64_t stripeUnit = 0;
    uint64_t stripeCount = 0;
    std::string poolset = "";
    // success
    {
        EXPECT_CALL(*client_, CreateFile(_, _, _, _, _, _))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        int ret = volumeServiceManager_->CreateFile(
            fileName,
            owner,
            size,
            stripeUnit,
            stripeCount,
            poolset);
        ASSERT_EQ(kErrCodeSuccess, ret);
    }
    // success 2
    {
        EXPECT_CALL(*client_, CreateFile(_, _, _, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::EXISTS));

        int ret = volumeServiceManager_->CreateFile(
            fileName,
            owner,
            size,
            stripeUnit,
            stripeCount,
            poolset);
        ASSERT_EQ(kErrCodeSuccess, ret);
    }
    // fail
    {
        EXPECT_CALL(*client_, CreateFile(_, _, _, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::AUTHFAIL));

        int ret = volumeServiceManager_->CreateFile(
            fileName,
            owner,
            size,
            stripeUnit,
            stripeCount,
            poolset);
        ASSERT_EQ(kErrCodeInvalidUser, ret);
    }
}

TEST_F(TestVolumeServiceManager, TestDeleteFile) {
    std::string fileName = "test";
    std::string owner = "test";
    // success
    {
        EXPECT_CALL(*client_, DeleteFile(_, _))
            .WillOnce(Return(LIBCURVE_ERROR::OK));

        int ret = volumeServiceManager_->DeleteFile(fileName, owner);
        ASSERT_EQ(kErrCodeSuccess, ret);
    }
    // success 2
    {
        EXPECT_CALL(*client_, DeleteFile(_, _))
            .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

        int ret = volumeServiceManager_->DeleteFile(fileName, owner);
        ASSERT_EQ(kErrCodeSuccess, ret);
    }
    // failed
    {
        EXPECT_CALL(*client_, DeleteFile(_, _))
            .WillOnce(Return(-LIBCURVE_ERROR::AUTHFAIL));

        int ret = volumeServiceManager_->DeleteFile(fileName, owner);
        ASSERT_EQ(kErrCodeInvalidUser, ret);
    }
}

TEST_F(TestVolumeServiceManager, TestGetFile) {
    // is INODE_PAGEFILE, success
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));
        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(FileInfoType::file, fileInfo.GetFileInfoType());
        ASSERT_EQ(FileInfoStatus::done, fileInfo.GetFileInfoStatus());
        ASSERT_EQ(100, fileInfo.GetProgress());
    }

    // is INODE_DIRECTORY, success
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_DIRECTORY;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));
        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(FileInfoType::directory, fileInfo.GetFileInfoType());
        ASSERT_EQ(FileInfoStatus::done, fileInfo.GetFileInfoStatus());
        ASSERT_EQ(100, fileInfo.GetProgress());
    }
    // is INODE_CLONE_PAGEFILE, success
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_CLONE_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 50;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(FileStatus::Flattening),
                            SetArgPointee<3>(progress),
                            Return(LIBCURVE_ERROR::OK)));

        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(FileInfoType::file, fileInfo.GetFileInfoType());
        ASSERT_EQ(FileInfoStatus::flattening, fileInfo.GetFileInfoStatus());
        ASSERT_EQ(progress, fileInfo.GetProgress());
    }
    // is INODE_CLONE_PAGEFILE, success 2
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_CLONE_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 0;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(FileStatus::Created),
                            SetArgPointee<3>(progress),
                            Return(LIBCURVE_ERROR::OK)));

        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(FileInfoType::file, fileInfo.GetFileInfoType());
        ASSERT_EQ(FileInfoStatus::unflattened, fileInfo.GetFileInfoStatus());
        ASSERT_EQ(progress, fileInfo.GetProgress());
    }
    // UNKNOWN file type, failed
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_SNAPSHOT_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));

        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeInternalError, ret);
    }
    // stat file failed
    {
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::AUTHFAIL));
        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeInvalidUser, ret);
    }
    // QueryFlattenStatus failed
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_CLONE_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 0;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::UNKNOWN));

        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeInternalError, ret);
    }
    // QueryFlattenStatus failed 2
    {
        FInfo_t finfo;
        finfo.filetype = FileType::INODE_CLONE_PAGEFILE;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfo),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 0;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

        FileInfo fileInfo;
        int ret = volumeServiceManager_->GetFile("test", "test", &fileInfo);
        ASSERT_EQ(kErrCodeFileNotExist, ret);
    }
}

TEST_F(TestVolumeServiceManager, TestListFile) {
    // success
    {
        std::vector<FInfo_t> finfos;
        EXPECT_CALL(*client_, ListDir(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfos),
                            Return(LIBCURVE_ERROR::OK)));

        std::vector<FileInfo> out;
        int ret = volumeServiceManager_->ListFile("/", "test", &out);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(0, out.size());
    }
    // success 2
    {
        std::vector<FInfo_t> finfos;
        FInfo_t finfo1;
        finfo1.filetype = FileType::INODE_DIRECTORY;
        finfos.push_back(finfo1);

        FInfo_t finfo2;
        finfo2.filetype = FileType::INODE_CLONE_PAGEFILE;
        finfos.push_back(finfo2);
        EXPECT_CALL(*client_, ListDir(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfos),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 0;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

        std::vector<FileInfo> out;
        int ret = volumeServiceManager_->ListFile("/", "test", &out);
        ASSERT_EQ(kErrCodeSuccess, ret);
        ASSERT_EQ(1, out.size());
    }
    // ListDir Failed
    {
        std::vector<FInfo_t> finfos;
        EXPECT_CALL(*client_, ListDir(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfos),
                            Return(-LIBCURVE_ERROR::AUTHFAIL)));

        std::vector<FileInfo> out;
        int ret = volumeServiceManager_->ListFile("/", "test", &out);
        ASSERT_EQ(kErrCodeInvalidUser, ret);
    }
    // success 2
    {
        std::vector<FInfo_t> finfos;
        FInfo_t finfo1;
        finfo1.filetype = FileType::INODE_DIRECTORY;
        finfos.push_back(finfo1);

        FInfo_t finfo2;
        finfo2.filetype = FileType::INODE_CLONE_PAGEFILE;
        finfos.push_back(finfo2);
        EXPECT_CALL(*client_, ListDir(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(finfos),
                            Return(LIBCURVE_ERROR::OK)));

        uint64_t progress = 0;
        EXPECT_CALL(*client_, QueryFlattenStatus(_, _, _, _))
            .WillOnce(Return(-LIBCURVE_ERROR::UNKNOWN));

        std::vector<FileInfo> out;
        int ret = volumeServiceManager_->ListFile("/dir1", "test", &out);
        ASSERT_EQ(kErrCodeInternalError, ret);
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve
