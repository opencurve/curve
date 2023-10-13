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
 * Project: nebd
 * Created Date: Monday January 20th 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/file_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "nebd/src/part2/file_entity.h"
#include "nebd/test/part2/mock_metafile_manager.h"
#include "nebd/test/part2/mock_request_executor.h"

namespace nebd {
namespace server {

const char testFile1[] = "test:/cinder/111";
const char testFile2[] = "test:/cinder/222";
const char unknownFile[] = "un:/cinder/666";

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

enum class RequestType {
    EXTEND = 0,
    GETINFO = 1,
    DISCARD = 2,
    AIOREAD = 3,
    AIOWRITE = 4,
    FLUSH = 5,
    INVALIDCACHE = 6,
};

class FileManagerTest : public ::testing::Test {
 public:
    void SetUp() {
        aioContext_ = new NebdServerAioContext();
        mockInstance_ = std::make_shared<NebdFileInstance>();
        executor_ = std::make_shared<MockRequestExecutor>();
        g_test_executor = executor_.get();
        metaFileManager_ = std::make_shared<MockMetaFileManager>();
        fileManager_ = std::make_shared<NebdFileManager>(metaFileManager_);
    }
    void TearDown() { delete aioContext_; }

    using TestTask = std::function<int(int)>;
    // Construct initial environment
    void InitEnv() {
        NebdFileMeta meta;
        meta.fd = 1;
        meta.fileName = testFile1;
        std::vector<NebdFileMeta> fileMetas;
        fileMetas.emplace_back(meta);

        EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
            .WillOnce(DoAll(SetArgPointee<0>(fileMetas), Return(0)));
        EXPECT_CALL(*executor_, Reopen(_, _)).WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _))
            .WillOnce(Return(0));
        ASSERT_EQ(fileManager_->Run(), 0);
    }

    void UnInitEnv() { ASSERT_EQ(fileManager_->Fini(), 0); }

    void ExpectCallRequest(RequestType type, int ret) {
        switch (type) {
            case RequestType::EXTEND:
                EXPECT_CALL(*executor_, Extend(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::GETINFO:
                EXPECT_CALL(*executor_, GetInfo(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::DISCARD:
                EXPECT_CALL(*executor_, Discard(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::AIOREAD:
                EXPECT_CALL(*executor_, AioRead(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::AIOWRITE:
                EXPECT_CALL(*executor_, AioWrite(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::FLUSH:
                EXPECT_CALL(*executor_, Flush(_, _)).WillOnce(Return(ret));
                break;
            case RequestType::INVALIDCACHE:
                EXPECT_CALL(*executor_, InvalidCache(_)).WillOnce(Return(ret));
                break;
        }
    }

    void RequestSuccssTest(RequestType type, TestTask task) {
        InitEnv();
        NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
        ASSERT_NE(nullptr, entity1);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

        // The file status is OPENED
        ExpectCallRequest(type, 0);
        ASSERT_EQ(0, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

        EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(false), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);
        // The file status is CLOSED
        EXPECT_CALL(*executor_, Open(testFile1, _))
            .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
            .WillOnce(Return(0));
        ExpectCallRequest(type, 0);
        ASSERT_EQ(0, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);
        UnInitEnv();
    }

    void RequestFailTest(RequestType type, TestTask task) {
        InitEnv();
        // Close the file
        NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
        ASSERT_NE(nullptr, entity1);
        EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(false), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // Open file failed
        EXPECT_CALL(*executor_, Open(testFile1, _)).WillOnce(Return(nullptr));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _)).Times(0);
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // Failed to update metadata file
        EXPECT_CALL(*executor_, Open(testFile1, _))
            .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // Failed to execute processing function
        EXPECT_CALL(*executor_, Open(testFile1, _))
            .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
            .WillOnce(Return(0));
        ExpectCallRequest(type, -1);
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

        // Set the file status to DESTROYED
        EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
        EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
            .WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(true), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::DESTROYED);
        EXPECT_CALL(*executor_, Open(testFile1, _)).Times(0);
        ASSERT_EQ(-1, task(1));

        // Delete files directly
        ASSERT_EQ(0, fileManager_->Close(1, true));
        ASSERT_EQ(nullptr, fileManager_->GetFileEntity(1));
        ASSERT_EQ(-1, task(1));
        UnInitEnv();
    }

 protected:
    std::shared_ptr<NebdFileManager> fileManager_;
    std::shared_ptr<MockMetaFileManager> metaFileManager_;
    std::shared_ptr<MockRequestExecutor> executor_;
    std::shared_ptr<NebdFileInstance> mockInstance_;
    NebdServerAioContext* aioContext_;
};

TEST_F(FileManagerTest, RunTest) {
    NebdFileMeta meta;
    meta.fd = 1;
    meta.fileName = testFile1;
    std::vector<NebdFileMeta> fileMetas;
    fileMetas.emplace_back(meta);

    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
        .WillOnce(DoAll(SetArgPointee<0>(fileMetas), Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _)).WillOnce(Return(0));
    ASSERT_EQ(fileManager_->Run(), 0);
    // Repeated run returns failed
    ASSERT_EQ(fileManager_->Run(), -1);

    // Verification results
    FileEntityMap entityMap = fileManager_->GetFileEntityMap();
    ASSERT_EQ(1, entityMap.size());
    ASSERT_NE(nullptr, entityMap[meta.fd]);
}

TEST_F(FileManagerTest, RunFailTest) {
    NebdFileMeta meta;
    meta.fd = 1;
    meta.fileName = testFile1;
    std::vector<NebdFileMeta> fileMetas;
    fileMetas.emplace_back(meta);

    // List file meta failed
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_)).WillOnce(Return(-1));
    ASSERT_EQ(fileManager_->Run(), -1);

    // Reopen failure does not affect Run success
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
        .WillOnce(DoAll(SetArgPointee<0>(fileMetas), Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _)).WillOnce(Return(nullptr));
    ASSERT_EQ(fileManager_->Run(), 0);
    ASSERT_EQ(fileManager_->Fini(), 0);

    // Failure to update metafile does not affect the success of Run
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
        .WillOnce(DoAll(SetArgPointee<0>(fileMetas), Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _)).WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Close(NotNull())).Times(1);
    ASSERT_EQ(fileManager_->Run(), 0);
}

TEST_F(FileManagerTest, OpenTest) {
    InitEnv();
    // Open a non-existent file
    EXPECT_CALL(*executor_, Open(testFile2, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
        .WillOnce(Return(0));
    int fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, 2);

    // Repeat open
    fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, 2);

    // open again with different openflags
    OpenFlags flags;
    ASSERT_LT(fileManager_->Open(testFile2, &flags), 0);

    NebdFileEntityPtr entity2 = fileManager_->GetFileEntity(2);
    ASSERT_NE(entity2, nullptr);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);

    EXPECT_CALL(*executor_, Close(_)).WillOnce(Return(0));
    ASSERT_EQ(entity2->Close(false), 0);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::CLOSED);
    // Open closed files, keep fd unchanged
    EXPECT_CALL(*executor_, Open(testFile2, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
        .WillOnce(Return(0));
    fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, 2);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);
}

TEST_F(FileManagerTest, OpenFailTest) {
    InitEnv();
    // Error calling backend open interface
    EXPECT_CALL(*executor_, Open(testFile2, _)).WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _)).Times(0);
    int fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, -1);

    // Persisting metadata information failed
    EXPECT_CALL(*executor_, Open(testFile2, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
        .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Close(_)).Times(1);
    fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, -1);

    // Open an illegal filename
    EXPECT_CALL(*executor_, Open(_, _)).Times(0);
    fd = fileManager_->Open(unknownFile, nullptr);
    ASSERT_EQ(fd, -1);
}

TEST_F(FileManagerTest, CloseTest) {
    InitEnv();
    // The specified fd does not exist, return success directly
    ASSERT_EQ(nullptr, fileManager_->GetFileEntity(2));
    ASSERT_EQ(0, fileManager_->Close(2, true));

    NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
    ASSERT_NE(nullptr, entity1);
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);
    // The file exists and its status is OPENED, while removeRecord is false
    EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1)).Times(0);
    ASSERT_EQ(0, fileManager_->Close(1, false));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

    // File exists, file status is CLOSED, removeRecord is false
    EXPECT_CALL(*executor_, Close(NotNull())).Times(0);
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1)).Times(0);
    ASSERT_EQ(0, fileManager_->Close(1, false));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

    // The file exists, the file status is CLOSED, and removeRecord is true
    EXPECT_CALL(*executor_, Close(NotNull())).Times(0);
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
        .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Close(1, true));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::DESTROYED);
    ASSERT_EQ(nullptr, fileManager_->GetFileEntity(1));

    EXPECT_CALL(*executor_, Open(testFile2, _)).WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
        .WillOnce(Return(0));
    int fd = fileManager_->Open(testFile2, nullptr);
    ASSERT_EQ(fd, 2);
    NebdFileEntityPtr entity2 = fileManager_->GetFileEntity(2);
    ASSERT_NE(entity2, nullptr);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);
    // File exists, file status is OPENED, removeRecord is true
    EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile2))
        .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Close(fd, true));
    ASSERT_EQ(nullptr, fileManager_->GetFileEntity(1));
}

TEST_F(FileManagerTest, CloseFailTest) {
    InitEnv();
    NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
    ASSERT_NE(nullptr, entity1);
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

    // Executor close failed
    EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(-1));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1)).Times(0);
    ASSERT_EQ(-1, fileManager_->Close(1, true));
    ASSERT_NE(nullptr, fileManager_->GetFileEntity(1));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

    // Remove file meta failed
    EXPECT_CALL(*executor_, Close(NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Close(1, true));
    ASSERT_NE(nullptr, fileManager_->GetFileEntity(1));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);
}

TEST_F(FileManagerTest, ExtendTest) {
    auto task = [&](int fd) -> int { return fileManager_->Extend(fd, 4096); };
    RequestSuccssTest(RequestType::EXTEND, task);
    RequestFailTest(RequestType::EXTEND, task);
}

TEST_F(FileManagerTest, GetInfoTest) {
    NebdFileInfo fileInfo;
    auto task = [&](int fd) -> int {
        return fileManager_->GetInfo(fd, &fileInfo);
    };
    RequestSuccssTest(RequestType::GETINFO, task);
    RequestFailTest(RequestType::GETINFO, task);
}

TEST_F(FileManagerTest, InvalidCacheTest) {
    auto task = [&](int fd) -> int { return fileManager_->InvalidCache(fd); };
    RequestSuccssTest(RequestType::INVALIDCACHE, task);
    RequestFailTest(RequestType::INVALIDCACHE, task);
}

TEST_F(FileManagerTest, AioReadTest) {
    NebdServerAioContext aioContext;
    auto task = [&](int fd) -> int {
        int ret = fileManager_->AioRead(fd, &aioContext);
        if (ret < 0) {
            if (aioContext.done != nullptr) {
                --ret;
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        } else {
            if (aioContext.done == nullptr) {
                --ret;
            } else {
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        }
        return ret;
    };
    RequestSuccssTest(RequestType::AIOREAD, task);
    RequestFailTest(RequestType::AIOREAD, task);
}

TEST_F(FileManagerTest, AioWriteTest) {
    NebdServerAioContext aioContext;
    auto task = [&](int fd) -> int {
        int ret = fileManager_->AioWrite(fd, &aioContext);
        if (ret < 0) {
            if (aioContext.done != nullptr) {
                --ret;
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        } else {
            if (aioContext.done == nullptr) {
                --ret;
            } else {
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        }
        return ret;
    };
    RequestSuccssTest(RequestType::AIOWRITE, task);
    RequestFailTest(RequestType::AIOWRITE, task);
}

TEST_F(FileManagerTest, DiscardTest) {
    NebdServerAioContext aioContext;
    auto task = [&](int fd) -> int {
        int ret = fileManager_->Discard(fd, &aioContext);
        if (ret < 0) {
            if (aioContext.done != nullptr) {
                --ret;
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        } else {
            if (aioContext.done == nullptr) {
                --ret;
            } else {
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        }
        return ret;
    };
    RequestSuccssTest(RequestType::DISCARD, task);
    RequestFailTest(RequestType::DISCARD, task);
}

TEST_F(FileManagerTest, FlushTest) {
    NebdServerAioContext aioContext;
    auto task = [&](int fd) -> int {
        int ret = fileManager_->Flush(fd, &aioContext);
        if (ret < 0) {
            if (aioContext.done != nullptr) {
                --ret;
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        } else {
            if (aioContext.done == nullptr) {
                --ret;
            } else {
                brpc::ClosureGuard doneGuard(aioContext.done);
                aioContext.done = nullptr;
            }
        }
        return ret;
    };
    RequestSuccssTest(RequestType::FLUSH, task);
    RequestFailTest(RequestType::FLUSH, task);
}

TEST_F(FileManagerTest, UpdateTimestampTest) {
    InitEnv();
    NebdFileEntityPtr entity = fileManager_->GetFileEntity(1);
    ASSERT_NE(nullptr, entity);
    ASSERT_EQ(1, entity->GetFd());

    ::usleep(1000);
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    ASSERT_NE(curTime, entity->GetFileTimeStamp());
    entity->UpdateFileTimeStamp(curTime);
    ASSERT_EQ(curTime, entity->GetFileTimeStamp());
    std::cout << fileManager_->DumpAllFileStatus();
}

}  // namespace server
}  // namespace nebd

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
