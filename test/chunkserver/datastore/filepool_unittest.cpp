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
 * File Created: Monday, 10th December 2018 3:22:12 pm
 * Author: tongguangxun
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <json/json.h>
#include <fcntl.h>
#include <climits>
#include <memory>

#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnPointee;
using ::testing::NotNull;
using ::testing::StrEq;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::ReturnArg;

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::chunkserver::FilePool;
using curve::chunkserver::FilePoolOptions;
using curve::chunkserver::FilePoolState_t;
using curve::common::kFilePoolMaigic;
using curve::chunkserver::FilePoolHelper;

class CSFilePool_test : public testing::Test {
 public:
    void SetUp() {
        fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        chunkFilePoolPtr_ = std::make_shared<FilePool>(fsptr);
        if (chunkFilePoolPtr_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        int count = 1;
        fsptr->Mkdir("./cspooltest/");
        std::string dirname = "./cspooltest/filePool";
        while (count < 51) {
            std::string  filename = "./cspooltest/filePool/"
                                  + std::to_string(count);
            fsptr->Mkdir("./cspooltest/filePool");
            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;

        int ret = FilePoolHelper::PersistEnCodeMetaInfo(
                                                    fsptr,
                                                    chunksize,
                                                    metapagesize,
                                                    dirname,
                                            "./cspooltest/filePool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        int fd = fsptr->Open("./cspooltest/filePool.meta2",
                             O_RDWR | O_CREAT);
        if (fd < 0) {
            return;
        }

        char* buffer = new char[2048];
        memset(buffer, 1, 2048);
        ret = fsptr->Write(fd, buffer, 0, 2048);
        if (ret != 2048) {
            delete[] buffer;
            return;
        }
        delete[] buffer;
        fsptr->Close(fd);
    }

    void TearDown() {
        std::vector<std::string> filename;
        fsptr->List("./cspooltest/filePool", &filename);
        for (auto iter : filename) {
            auto path = "./cspooltest/filePool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
        fsptr->Delete("./cspooltest/filePool");
        fsptr->Delete("./cspooltest/filePool.meta");
        fsptr->Delete("./cspooltest/filePool.meta2");
        fsptr->Delete("./cspooltest");
        chunkFilePoolPtr_->UnInitialize();
    }

    std::shared_ptr<FilePool>  chunkFilePoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
};

bool CheckFileOpenOrNot(const std::string& filename) {
    std::string syscmd;
    syscmd.append("lsof ").append(filename);
    FILE * fp;
    char buffer[4096];
    memset(buffer, 0, 4096);
    fp = popen(syscmd.c_str(), "r");
    fgets(buffer, sizeof(buffer), fp);
    pclose(fp);

    std::string out(buffer, 4096);
    return out.find("No such file or directory") != out.npos;
}

TEST_F(CSFilePool_test, InitializeTest) {
    std::string filePool = "./cspooltest/filePool.meta";

    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());

    // initialize
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    ASSERT_EQ(50, chunkFilePoolPtr_->Size());
    // 初始化阶段会扫描FilePool内的所有文件，在扫描结束之后需要关闭这些文件
    // 防止过多的文件描述符被占用
    ASSERT_FALSE(CheckFileOpenOrNot("./cspooltest/filePool/1"));
    ASSERT_FALSE(CheckFileOpenOrNot("./cspooltest/filePool/2"));
    cfop.fileSize = 8192;
    cfop.metaPageSize = 4096;
    // test meta content wrong
    ASSERT_FALSE(chunkFilePoolPtr_->Initialize(cfop));
    cfop.fileSize = 8192;
    cfop.metaPageSize = 4096;
    ASSERT_FALSE(chunkFilePoolPtr_->Initialize(cfop));
    // invalid file name
    std::string  filename = "./cspooltest/filePool/a";
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
    char data[8192];
    memset(data, 'a', 8192);
    fsptr->Write(fd, data, 0, 8192);
    fsptr->Close(fd);
    ASSERT_FALSE(chunkFilePoolPtr_->Initialize(cfop));
    // test meta file wrong
    filePool = "./cspooltest/filePool.meta2";
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    ASSERT_FALSE(chunkFilePoolPtr_->Initialize(cfop));
    // test meta file not exist
    filePool = "./cspooltest/FilePool.meta3";
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    ASSERT_FALSE(chunkFilePoolPtr_->Initialize(cfop));

    fsptr->Delete("./cspooltest/filePool/a");
    fsptr->Delete("./cspooltest/filePool.meta3");
}

TEST_F(CSFilePool_test, GetFileTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    // test get chunk success
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(-1, chunkFilePoolPtr_->GetFile("./new_exit", metapage));
    ASSERT_EQ(-2, fsptr->Delete("./new_exit"));
    chunkFilePoolPtr_->Initialize(cfop);
    ASSERT_EQ(50, chunkFilePoolPtr_->Size());
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("./new1", metapage));
    ASSERT_EQ(49, chunkFilePoolPtr_->Size());
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    int fd = fsptr->Open("./new1", O_RDWR);
    char data[4096];
    ASSERT_GE(fd, 0);
    int len = fsptr->Read(fd, data, 0, 4096);
    ASSERT_EQ(4096, len);
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(data[i], '1');
    }
    ASSERT_EQ(0, fsptr->Close(fd));
    ASSERT_EQ(0, fsptr->Delete("./new1"));

    // test get chunk success
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("./new2", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new2"));
    ASSERT_NE(49, chunkFilePoolPtr_->Size());
    ASSERT_EQ(0, fsptr->Delete("./new2"));
}

TEST_F(CSFilePool_test, RecycleFileTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());

    chunkFilePoolPtr_->Initialize(cfop);
    FilePoolState_t currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.preallocatedChunksLeft);
    ASSERT_EQ(50, chunkFilePoolPtr_->Size());
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("./new1", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    ASSERT_EQ(49, chunkFilePoolPtr_->Size());

    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(49, currentStat.preallocatedChunksLeft);

    chunkFilePoolPtr_->RecycleFile("./new1");
    ASSERT_EQ(50, chunkFilePoolPtr_->Size());

    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.preallocatedChunksLeft);

    ASSERT_FALSE(fsptr->FileExists("./new1"));
    ASSERT_TRUE(fsptr->FileExists("./cspooltest/filePool/4"));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/filePool/4"));
}

TEST(CSFilePool, GetFileDirectlyTest) {
    std::shared_ptr<FilePool>  chunkFilePoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
    fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

    // create chunkfile in chunkfile pool dir
    // if chunkfile pool 的getFileFromPool开关关掉了，那么
    // FilePool的size是一直为0，不会从pool目录中找
    std::string  filename = "./cspooltest/filePool/1000";
    fsptr->Mkdir("./cspooltest/filePool");
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);

    char data[8192];
    memset(data, 'a', 8192);
    ASSERT_EQ(8192, fsptr->Write(fd, data, 0, 8192));
    fsptr->Close(fd);
    ASSERT_TRUE(fsptr->FileExists("./cspooltest/filePool/1000"));

    FilePoolOptions cspopt;
    cspopt.getFileFromPool = false;
    cspopt.fileSize = 16 * 1024;
    cspopt.metaPageSize = 4 * 1024;
    cspopt.metaFileSize = 4 * 1024;
    cspopt.retryTimes = 5;
    strcpy(cspopt.filePoolDir, "./cspooltest/filePool");                             // NOLINT

    chunkFilePoolPtr_ = std::make_shared<FilePool>(fsptr);
    if (chunkFilePoolPtr_ == nullptr) {
        LOG(FATAL) << "allocate chunkfile pool failed!";
    }
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cspopt));
    ASSERT_EQ(0, chunkFilePoolPtr_->Size());

    // 测试获取chunk，chunkfile pool size不变一直为0
    char metapage[4096];
    memset(metapage, '1', 4096);

    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("./new1", metapage));
    ASSERT_EQ(0, chunkFilePoolPtr_->Size());

    ASSERT_TRUE(fsptr->FileExists("./new1"));
    fd = fsptr->Open("./new1", O_RDWR);
    ASSERT_GE(fd, 0);

    char buf[4096];
    ASSERT_EQ(4096, fsptr->Read(fd, buf, 0, 4096));
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(buf[i], '1');
    }

    // 测试回收chunk,文件被删除，FilePool Size不受影响
    chunkFilePoolPtr_->RecycleFile("./new1");
    ASSERT_EQ(0, chunkFilePoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists("./new1"));

    // 删除测试文件及目录
    ASSERT_EQ(0, fsptr->Close(fd));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/filePool/1000"));
    ASSERT_EQ(0, fsptr->Delete("./cspooltest/filePool"));
    chunkFilePoolPtr_->UnInitialize();
}
