/*
 * Project: nebd
 * Created Date: Monday January 20th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/part2/file_manager.h"
#include "src/part2/file_entity.h"
#include "tests/part2/mock_request_executor.h"
#include "tests/part2/mock_metafile_manager.h"

namespace nebd {
namespace server {

const char testFile1[] = "test:/cinder/111";
const char testFile2[] = "test:/cinder/222";
const char unknownFile[] = "un:/cinder/666";

using ::testing::_;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
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
    void TearDown() {
        delete aioContext_;
    }

    using TestTask = std::function<int(int)>;
    // 构造初始环境
    void InitEnv() {
        NebdFileMeta meta;
        meta.fd = 1;
        meta.fileName = testFile1;
        std::vector<NebdFileMeta> fileMetas;
        fileMetas.emplace_back(meta);

        EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
        .WillOnce(DoAll(SetArgPointee<0>(fileMetas),
                        Return(0)));
        EXPECT_CALL(*executor_, Reopen(_, _))
        .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _))
        .WillOnce(Return(0));
        ASSERT_EQ(fileManager_->Run(), 0);
    }

    void UnInitEnv() {
        ASSERT_EQ(fileManager_->Fini(), 0);
    }

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

        // 文件状态为OPENED
        ExpectCallRequest(type, 0);
        ASSERT_EQ(0, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

        EXPECT_CALL(*executor_, Close(NotNull()))
        .WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(false), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);
        // 文件状态为CLOSED
        EXPECT_CALL(*executor_, Open(testFile1))
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
        // 将文件close
        NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
        ASSERT_NE(nullptr, entity1);
        EXPECT_CALL(*executor_, Close(NotNull()))
        .WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(false), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // open文件失败
        EXPECT_CALL(*executor_, Open(testFile1))
        .WillOnce(Return(nullptr));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
        .Times(0);
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // 更新元数据文件失败
        EXPECT_CALL(*executor_, Open(testFile1))
        .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
        .WillOnce(Return(-1));
        EXPECT_CALL(*executor_, Close(NotNull()))
        .WillOnce(Return(0));
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

        // 执行处理函数失败
        EXPECT_CALL(*executor_, Open(testFile1))
        .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile1, _))
        .WillOnce(Return(0));
        ExpectCallRequest(type, -1);
        ASSERT_EQ(-1, task(1));
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

        // 将文件状态置为DESTROYED
        EXPECT_CALL(*executor_, Close(NotNull()))
        .WillOnce(Return(0));
        EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
        .WillOnce(Return(0));
        ASSERT_EQ(entity1->Close(true), 0);
        ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::DESTROYED);
        EXPECT_CALL(*executor_, Open(testFile1))
        .Times(0);
        ASSERT_EQ(-1, task(1));

        // 直接将文件删除
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
    .WillOnce(DoAll(SetArgPointee<0>(fileMetas),
                    Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _))
    .WillOnce(Return(0));
    ASSERT_EQ(fileManager_->Run(), 0);
    // 重复run返回失败
    ASSERT_EQ(fileManager_->Run(), -1);

    // 校验结果
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

    // list file meta失败
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
    .WillOnce(Return(-1));
    ASSERT_EQ(fileManager_->Run(), -1);

    // reopen失败不影响Run成功
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
    .WillOnce(DoAll(SetArgPointee<0>(fileMetas),
                    Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _))
    .WillOnce(Return(nullptr));
    ASSERT_EQ(fileManager_->Run(), 0);
    ASSERT_EQ(fileManager_->Fini(), 0);

    // 更新metafile失败不影响Run成功
    EXPECT_CALL(*metaFileManager_, ListFileMeta(_))
    .WillOnce(DoAll(SetArgPointee<0>(fileMetas),
                    Return(0)));
    EXPECT_CALL(*executor_, Reopen(_, _))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(_, _))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Close(NotNull()))
    .Times(1);
    ASSERT_EQ(fileManager_->Run(), 0);
}

TEST_F(FileManagerTest, OpenTest) {
    InitEnv();
    // open一个不存在的文件
    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
    .WillOnce(Return(0));
    int fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, 2);

    // 重复open
    fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, 2);

    NebdFileEntityPtr entity2 = fileManager_->GetFileEntity(2);
    ASSERT_NE(entity2, nullptr);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);

    EXPECT_CALL(*executor_, Close(_))
    .WillOnce(Return(0));
    ASSERT_EQ(entity2->Close(false), 0);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::CLOSED);
    // open 已经close的文件, fd不变
    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
    .WillOnce(Return(0));
    fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, 2);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);
}

TEST_F(FileManagerTest, OpenFailTest) {
    InitEnv();
    // 调用后端open接口时出错
    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
    .Times(0);
    int fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, -1);

    // 持久化元数据信息失败
    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Close(_))
    .Times(1);
    fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, -1);

    // Open一个非法的filename
    EXPECT_CALL(*executor_, Open(_))
    .Times(0);
    fd = fileManager_->Open(unknownFile);
    ASSERT_EQ(fd, -1);
}

TEST_F(FileManagerTest, CloseTest) {
    InitEnv();
    // 指定的fd不存在,直接返回成功
    ASSERT_EQ(nullptr, fileManager_->GetFileEntity(2));
    ASSERT_EQ(0, fileManager_->Close(2, true));

    NebdFileEntityPtr entity1 = fileManager_->GetFileEntity(1);
    ASSERT_NE(nullptr, entity1);
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);
    // 文件存在，且文件状态为OPENED，removeRecord为false
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
    .Times(0);
    ASSERT_EQ(0, fileManager_->Close(1, false));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

    // 文件存在，文件状态为CLOSED，removeRecord为false
    EXPECT_CALL(*executor_, Close(NotNull()))
    .Times(0);
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
    .Times(0);
    ASSERT_EQ(0, fileManager_->Close(1, false));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);

    // 文件存在，文件状态为CLOSED，removeRecord为true
    EXPECT_CALL(*executor_, Close(NotNull()))
    .Times(0);
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Close(1, true));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::DESTROYED);
    ASSERT_EQ(nullptr, fileManager_->GetFileEntity(1));

    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileMeta(testFile2, _))
    .WillOnce(Return(0));
    int fd = fileManager_->Open(testFile2);
    ASSERT_EQ(fd, 2);
    NebdFileEntityPtr entity2 = fileManager_->GetFileEntity(2);
    ASSERT_NE(entity2, nullptr);
    ASSERT_EQ(entity2->GetFileStatus(), NebdFileStatus::OPENED);
    // 文件存在，文件状态为OPENED，removeRecord为true
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
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

    // executor close 失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(-1));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Close(1, true));
    ASSERT_NE(nullptr, fileManager_->GetFileEntity(1));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::OPENED);

    // remove file meta 失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileMeta(testFile1))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Close(1, true));
    ASSERT_NE(nullptr, fileManager_->GetFileEntity(1));
    ASSERT_EQ(entity1->GetFileStatus(), NebdFileStatus::CLOSED);
}

TEST_F(FileManagerTest, ExtendTest) {
    auto task = [&](int fd)->int {
        return fileManager_->Extend(fd, 4096);
    };
    RequestSuccssTest(RequestType::EXTEND, task);
    RequestFailTest(RequestType::EXTEND, task);
}

TEST_F(FileManagerTest, GetInfoTest) {
    NebdFileInfo fileInfo;
    auto task = [&](int fd)->int {
        return fileManager_->GetInfo(fd, &fileInfo);
    };
    RequestSuccssTest(RequestType::GETINFO, task);
    RequestFailTest(RequestType::GETINFO, task);
}

TEST_F(FileManagerTest, InvalidCacheTest) {
    auto task = [&](int fd)->int {
        return fileManager_->InvalidCache(fd);
    };
    RequestSuccssTest(RequestType::INVALIDCACHE, task);
    RequestFailTest(RequestType::INVALIDCACHE, task);
}

TEST_F(FileManagerTest, AioReadTest) {
    NebdServerAioContext aioContext;
    auto task = [&](int fd)->int {
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
    auto task = [&](int fd)->int {
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
    auto task = [&](int fd)->int {
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
    auto task = [&](int fd)->int {
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

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
