/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Jun 22 2021
 * Author: huyao
 */


#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/test/client/mock_disk_cache_manager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "src/common/curve_define.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"


namespace curvefs {
namespace client {
using ::curve::common::kMB;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

class ClientS3AdaptorTest2 : public testing::Test {
 protected:
    ClientS3AdaptorTest2() {}
    ~ClientS3AdaptorTest2() {}
    void SetUp() override {
        s3ClientAdaptor_ = std::make_shared<S3ClientAdaptorImpl>();
        mockInodeManager_ = std::make_shared<MockInodeCacheManager>();
        mockDiskcacheManagerImpl_ =
            std::make_shared<MockDiskCacheManagerImpl>();
        mockFsCacheManager_ = std::make_shared<MockFsCacheManager>();

        s3ClientAdaptor_->InitForTest(mockDiskcacheManagerImpl_,
                                      mockFsCacheManager_, mockInodeManager_);
    }

    void TearDown() override {}

    void GetInode(curvefs::metaserver::Inode *inode) {
        inode->set_inodeid(1);
        inode->set_fsid(2);
        inode->set_length(0);
        inode->set_ctime(1623835517);
        inode->set_ctime_ns(1623835517);
        inode->set_mtime(1623835517);
        inode->set_atime(1623835517);
        inode->set_atime_ns(1623835517);
        inode->set_uid(1);
        inode->set_gid(1);
        inode->set_mode(1);
        inode->set_nlink(1);
        inode->set_type(curvefs::metaserver::FsFileType::TYPE_S3);
    }

 protected:
    std::shared_ptr<S3ClientAdaptorImpl> s3ClientAdaptor_;
    std::shared_ptr<MockInodeCacheManager> mockInodeManager_;
    std::shared_ptr<MockDiskCacheManagerImpl> mockDiskcacheManagerImpl_;
    std::shared_ptr<MockFsCacheManager> mockFsCacheManager_;
};


TEST_F(ClientS3AdaptorTest2, FlushAllCache_no_filecachaeManager) {
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest2, FlushAllCache_flush_fail) {
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest2, FlushAllCache_with_no_cache) {
    s3ClientAdaptor_->SetDiskCache(DiskCacheType::Disable);

    LOG(INFO) << "############ case1: do not find file cache";
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));

    LOG(INFO) << "############ case2: find file cache";
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest2, FlushAllCache_with_cache) {
    s3ClientAdaptor_->SetDiskCache(DiskCacheType::ReadWrite);

    LOG(INFO) << "############ case1: clear write cache fail";
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, UploadWriteCacheByInode(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, s3ClientAdaptor_->FlushAllCache(1));

    LOG(INFO)
        << "############ case2: clear write cache ok, update write cache ok ";
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, UploadWriteCacheByInode(_))
        .WillOnce(Return(0));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

}  // namespace client
}  // namespace curvefs
