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

#include <fcntl.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <climits>
#include <memory>
#include <thread>

#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/fs/local_filesystem.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::ReturnPointee;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using curve::chunkserver::FilePool;
using curve::chunkserver::FilePoolHelper;
using curve::chunkserver::FilePoolOptions;
using curve::chunkserver::FilePoolState;
using curve::chunkserver::FilePoolMeta;
using curve::common::kFilePoolMagic;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

#define TOTAL_FILE_NUM 10000
#define THREAD_NUM 1000
#define FILE_NUM_PER_THEAD 10

const char POOL1_DIR[] = "./cspooltest/pool1/";
const char POOL2_DIR[] = "./cspooltest/pool2/";
const char FILEPOOL_DIR[] = "./cspooltest/filePool/";

class CSFilePool_test : public testing::TestWithParam<bool> {
 public:
    void SetUp() {
        fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        chunkFilePoolPtr_ = std::make_shared<FilePool>(fsptr);
        if (chunkFilePoolPtr_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        /* remove all pool dir */
        if (fsptr->DirExists(FILEPOOL_DIR)) {
            ASSERT_EQ(0, fsptr->Delete(FILEPOOL_DIR));
        }
        if (fsptr->DirExists(POOL1_DIR)) {
            ASSERT_EQ(0, fsptr->Delete(POOL1_DIR));
        }
        if (fsptr->DirExists(POOL2_DIR)) {
            ASSERT_EQ(0, fsptr->Delete(POOL2_DIR));
        }

        auto createFile = [&](int num, bool isCleaned) {
            std::string filename = FILEPOOL_DIR + std::to_string(num);
            char data[8192];
            if (isCleaned) {
                filename = filename + ".clean";
                memset(data, 0, 8192);
            } else {
                memset(data, 'a', 8192);
            }

            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            ASSERT_GT(fd, 0);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
        };

        ASSERT_EQ(0, fsptr->Mkdir("./cspooltest/"));
        ASSERT_EQ(0, fsptr->Mkdir(FILEPOOL_DIR));
        for (int i = 1; i <= 100; i++) {
            createFile(i, i > 50);
        }

        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;
        uint32_t blockSize = 4096;

        FilePoolMeta meta;
        if (GetParam()) {
            meta =
                FilePoolMeta{chunksize, metapagesize, blockSize, FILEPOOL_DIR};
        } else {
            meta = FilePoolMeta{chunksize, metapagesize, FILEPOOL_DIR};
        }

        int ret = FilePoolHelper::PersistEnCodeMetaInfo(
            fsptr, meta, "./cspooltest/filePool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        int fd = fsptr->Open("./cspooltest/filePool.meta2", O_RDWR | O_CREAT);
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
        ASSERT_TRUE(chunkFilePoolPtr_->StopCleaning());
        fsptr->Delete("./cspooltest");
        chunkFilePoolPtr_->UnInitialize();
    }

    std::shared_ptr<FilePool> chunkFilePoolPtr_;
    std::shared_ptr<LocalFileSystem> fsptr;
};

bool CheckFileOpenOrNot(const std::string& filename) {
    std::string syscmd;
    syscmd.append("lsof ").append(filename);
    FILE* fp;
    char buffer[4096];
    memset(buffer, 0, 4096);
    fp = popen(syscmd.c_str(), "r");
    fgets(buffer, sizeof(buffer), fp);
    pclose(fp);

    std::string out(buffer, 4096);
    return out.find("No such file or directory") != out.npos;
}

TEST_P(CSFilePool_test, InitializeTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    const std::string filePoolPath = FILEPOOL_DIR;

    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.blockSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    memcpy(cfop.filePoolDir, filePoolPath.c_str(), filePoolPath.size());

    // initialize
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());
    //During the initialization phase, all files in the FilePool will be scanned, and after the scan is completed, these files need to be closed
    //Prevent excessive file descriptors from being occupied
    ASSERT_FALSE(CheckFileOpenOrNot(filePoolPath + "1"));
    ASSERT_FALSE(CheckFileOpenOrNot(filePoolPath + "2"));
    ASSERT_FALSE(CheckFileOpenOrNot(filePoolPath + "50.clean"));
    ASSERT_FALSE(CheckFileOpenOrNot(filePoolPath + "100.clean"));

    std::string filename = filePoolPath + "a";
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

    fsptr->Delete(filePoolPath + "a");
    fsptr->Delete("./cspooltest/filePool.meta3");
}

TEST_P(CSFilePool_test, GetFileTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.blockSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    strncpy(cfop.filePoolDir, FILEPOOL_DIR, strlen(FILEPOOL_DIR) + 1);

    char metapage[4096];
    memset(metapage, '1', 4096);

    // CASE 1: chunk file pool is empty
    ASSERT_EQ(-1, chunkFilePoolPtr_->GetFile("test0", metapage));
    ASSERT_EQ(-2, fsptr->Delete("test0"));

    // CASE 2: get dirty chunk
    auto checkBytes = [this](const std::string& filename,
                             char byte,
                             bool isCleaned = false) {
        ASSERT_TRUE(fsptr->FileExists(filename));
        int fd = fsptr->Open(filename, O_RDWR);
        ASSERT_GE(fd, 0);

        char data[4096];
        int len = fsptr->Read(fd, data, 0, 4096);
        ASSERT_EQ(4096, len);

        for (int i = 0; i < 4096; i++) {
            ASSERT_EQ(data[i], byte);
        }

        if (isCleaned) {
            for (int i = 4096; i < 8092; i++) {
                ASSERT_EQ(data[i], '\0');
            }
        }

        ASSERT_EQ(0, fsptr->Close(fd));
        ASSERT_EQ(0, fsptr->Delete(filename));
    };

    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    auto currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());

    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("test1", metapage));
    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(49, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(99, chunkFilePoolPtr_->Size());
    checkBytes("test1", '1');

    // CASE 3: get clean chunk
    memset(metapage, '2', 4096);
    int ret = chunkFilePoolPtr_->GetFile("test2", metapage, true);
    ASSERT_EQ(0, ret);
    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(49, currentStat.dirtyChunksLeft);
    ASSERT_EQ(49, currentStat.cleanChunksLeft);
    ASSERT_EQ(98, chunkFilePoolPtr_->Size());
    checkBytes("test2", '2');
}

TEST_P(CSFilePool_test, RecycleFileTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    const std::string filePoolPath = FILEPOOL_DIR;
    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.blockSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    strncpy(cfop.filePoolDir, FILEPOOL_DIR, strlen(FILEPOOL_DIR) + 1);

    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    FilePoolState currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());

    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(0, chunkFilePoolPtr_->GetFile("./new1", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(49, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(99, chunkFilePoolPtr_->Size());

    chunkFilePoolPtr_->RecycleFile("./new1");
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());
    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);

    ASSERT_FALSE(fsptr->FileExists("./new1"));
    ASSERT_TRUE(fsptr->FileExists(filePoolPath + "4"));
    ASSERT_EQ(0, fsptr->Delete(filePoolPath + "4"));
}

TEST_P(CSFilePool_test, UsePoolConcurrentGetAndRecycle) {
    std::string filePool = "./cspooltest/filePool.meta";
    const std::string filePoolPath = FILEPOOL_DIR;
    FilePoolOptions cfop;
    memcpy(cfop.filePoolDir, filePoolPath.c_str(), filePoolPath.size());
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.blockSize = 4096;
    cfop.getFileFromPool = true;
    cfop.retryTimes = 1;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    strncpy(cfop.filePoolDir, FILEPOOL_DIR, strlen(FILEPOOL_DIR) + 1);
    /* step 1. prepare file for filePool and pool2 */
    int count = 1;

    // NOTE: there are 50 clean chunks in pool already
    while (count <= TOTAL_FILE_NUM - 50) {
        std::string filename = filePoolPath + std::to_string(count);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        char data[8192];
        memset(data, 'a', 8192);
        fsptr->Write(fd, data, 0, 8192);
        fsptr->Close(fd);
        count++;
    }

    fsptr->Mkdir(POOL1_DIR);
    fsptr->Mkdir(POOL2_DIR);
    count = 1;
    while (count <= TOTAL_FILE_NUM) {
        std::string filename = POOL2_DIR + std::to_string(count);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        char data[8192];
        memset(data, 'b', 8192);
        fsptr->Write(fd, data, 0, 8192);
        fsptr->Close(fd);
        count++;
    }

    /* step 2. init filepool */
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));

    /* step 3. start multiple threads, get files from filePool to pool1  */
    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_NUM; i++) {
        int id = i;
        auto task = [this, id]() {
            char metapage[4096];
            memset(metapage, '1', 4096);
            for (int filenum = id * FILE_NUM_PER_THEAD + 1;
                 filenum <= (id * FILE_NUM_PER_THEAD + FILE_NUM_PER_THEAD);
                 filenum++) {
                ASSERT_EQ(0,
                          chunkFilePoolPtr_->GetFile(
                              POOL1_DIR + std::to_string(filenum), metapage));
            }
        };
        std::thread currThread(task);
        threads.push_back(std::move(currThread));
    }

    /* step 4. start multiple thread, recycle files from pool2 to filePool */
    for (int i = 0; i < THREAD_NUM; i++) {
        int id = i;
        auto task = [this, id]() {
            for (int filenum = id * FILE_NUM_PER_THEAD + 1;
                 filenum <= (id * FILE_NUM_PER_THEAD + FILE_NUM_PER_THEAD);
                 filenum++) {
                ASSERT_EQ(0, chunkFilePoolPtr_->RecycleFile(
                                 POOL2_DIR + std::to_string(filenum)));
            }
        };
        std::thread currThread(task);
        threads.push_back(std::move(currThread));
    }

    for (auto iter = threads.begin(); iter != threads.end(); iter++) {
        iter->join();
    }

    /* step 5. verify file numbers in filePool, pool1 and poo2 */
    {
        std::vector<std::string> filename;
        fsptr->List(filePoolPath, &filename);
        LOG(INFO) << "file Pool size=" << filename.size();
        ASSERT_EQ(filename.size(), TOTAL_FILE_NUM);
    }
    {
        std::vector<std::string> filename;
        fsptr->List(POOL1_DIR, &filename);
        LOG(INFO) << "pool1 size=" << filename.size();
        ASSERT_EQ(filename.size(), TOTAL_FILE_NUM);
    }
    {
        std::vector<std::string> filename;
        fsptr->List(POOL2_DIR, &filename);
        LOG(INFO) << "pool2 size=" << filename.size();
        ASSERT_EQ(filename.size(), 0);
    }
}

TEST_P(CSFilePool_test, WithoutPoolConcurrentGetAndRecycle) {
    std::string filePool = "./cspooltest/filePool.meta";
    const std::string filePoolPath = FILEPOOL_DIR;
    FilePoolOptions cfop;
    memcpy(cfop.filePoolDir, filePoolPath.c_str(), filePoolPath.size());
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.getFileFromPool = false;
    cfop.retryTimes = 1;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());

    /* step 1. prepare file for filePool and pool2 */
    ASSERT_EQ(0, fsptr->Delete(filePoolPath));
    fsptr->Mkdir(filePoolPath);
    fsptr->Mkdir(POOL1_DIR);
    fsptr->Mkdir(POOL2_DIR);
    int count = 1;
    while (count <= TOTAL_FILE_NUM) {
        std::string filename = POOL2_DIR + std::to_string(count);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        char data[8192];
        memset(data, 'b', 8192);
        fsptr->Write(fd, data, 0, 8192);
        fsptr->Close(fd);
        count++;
    }

    /* step 2. init filepool */
    chunkFilePoolPtr_->Initialize(cfop);

    /* step 3. start multiple threads, get files from filePool to pool1  */
    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_NUM; i++) {
        int id = i;
        auto task = [this, id]() {
            char metapage[4096];
            memset(metapage, '1', 4096);
            for (int filenum = id * FILE_NUM_PER_THEAD + 1;
                 filenum <= (id * FILE_NUM_PER_THEAD + FILE_NUM_PER_THEAD);
                 filenum++) {
                ASSERT_EQ(0,
                          chunkFilePoolPtr_->GetFile(
                              POOL1_DIR + std::to_string(filenum), metapage));
            }
        };
        std::thread currThread(task);
        threads.push_back(std::move(currThread));
    }

    /* step 4. start multiple thread, recycle files from pool2 to filePool */
    for (int i = 0; i < THREAD_NUM; i++) {
        int id = i;
        auto task = [this, id]() {
            for (int filenum = id * FILE_NUM_PER_THEAD + 1;
                 filenum <= (id * FILE_NUM_PER_THEAD + FILE_NUM_PER_THEAD);
                 filenum++) {
                ASSERT_EQ(0, chunkFilePoolPtr_->RecycleFile(
                                 POOL2_DIR + std::to_string(filenum)));
            }
        };
        std::thread currThread(task);
        threads.push_back(std::move(currThread));
    }

    for (auto iter = threads.begin(); iter != threads.end(); iter++) {
        iter->join();
    }

    /* step 5. verify file numbers in filePool, pool1 and poo2 */
    {
        std::vector<std::string> filename;
        fsptr->List(filePoolPath, &filename);
        LOG(INFO) << "file Pool size=" << filename.size();
        ASSERT_EQ(filename.size(), 0);
    }
    {
        std::vector<std::string> filename;
        fsptr->List(POOL1_DIR, &filename);
        LOG(INFO) << "pool1 size=" << filename.size();
        ASSERT_EQ(filename.size(), TOTAL_FILE_NUM);
    }
    {
        std::vector<std::string> filename;
        fsptr->List(POOL2_DIR, &filename);
        LOG(INFO) << "pool2 size=" << filename.size();
        ASSERT_EQ(filename.size(), 0);
    }
}

TEST_P(CSFilePool_test, CleanChunkTest) {
    std::string filePool = "./cspooltest/filePool.meta";
    const std::string filePoolPath = FILEPOOL_DIR;

    FilePoolOptions cfop;
    cfop.fileSize = 4096;
    cfop.metaPageSize = 4096;
    cfop.blockSize = 4096;
    memcpy(cfop.metaPath, filePool.c_str(), filePool.size());
    strncpy(cfop.filePoolDir, FILEPOOL_DIR, strlen(FILEPOOL_DIR) + 1);

    // CASE 1: initialize
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    auto currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());

    // CASE 2: disable clean, nothing happen
    sleep(3);
    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_EQ(50, currentStat.dirtyChunksLeft);
    ASSERT_EQ(50, currentStat.cleanChunksLeft);
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());

    // CASE 3: enable clean, set iops=2
    chunkFilePoolPtr_->UnInitialize();
    cfop.needClean = true;
    cfop.iops4clean = 2;  // clean 1 chunk every second
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cfop));
    ASSERT_TRUE(chunkFilePoolPtr_->StartCleaning());
    sleep(3);
    ASSERT_TRUE(chunkFilePoolPtr_->StopCleaning());

    currentStat = chunkFilePoolPtr_->GetState();
    ASSERT_GE(currentStat.dirtyChunksLeft, 46);
    ASSERT_LE(currentStat.dirtyChunksLeft, 47);
    ASSERT_GE(currentStat.cleanChunksLeft, 53);
    ASSERT_LE(currentStat.cleanChunksLeft, 54);
    ASSERT_EQ(100, chunkFilePoolPtr_->Size());

    // CASE 4: get clean chunk
    char metapage[4096], data[8092];
    memset(metapage, '2', sizeof(metapage));
    for (int i = 1; i <= 100; i++) {
        std::string filename = "test" + std::to_string(i);

        int ret = chunkFilePoolPtr_->GetFile(filename, metapage, true);
        ASSERT_EQ(0, ret);
        ASSERT_EQ(100 - i, chunkFilePoolPtr_->Size());
        ASSERT_TRUE(fsptr->FileExists(filename));

        int fd = fsptr->Open(filename, O_RDWR);
        ASSERT_GE(fd, 0);
        int len = fsptr->Read(fd, data, 0, 8092);
        ASSERT_EQ(8092, len);

        for (int j = 0; j < 4096; j++) ASSERT_EQ(data[j], '2');
        for (int j = 4096; j < 8092; j++) ASSERT_EQ(data[j], '\0');

        ASSERT_EQ(0, fsptr->Close(fd));
        ASSERT_EQ(0, fsptr->Delete(filename));
    }
}

INSTANTIATE_TEST_CASE_P(CSFilePoolTest,
                        CSFilePool_test,
                        ::testing::Values(false, true));

TEST(CSFilePool, GetFileDirectlyTest) {
    std::shared_ptr<FilePool> chunkFilePoolPtr_;
    std::shared_ptr<LocalFileSystem> fsptr;
    fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    const std::string filePoolPath = FILEPOOL_DIR;
    // create chunkfile in chunkfile pool dir
    //If the getFileFromPool switch of the chunkfile pool is turned off, then
    //The size of FilePool is always 0 and will not be found in the pool directory
    std::string filename = filePoolPath + "1000";
    fsptr->Mkdir(filePoolPath);
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);

    char data[8192];
    memset(data, 'a', 8192);
    ASSERT_EQ(8192, fsptr->Write(fd, data, 0, 8192));
    fsptr->Close(fd);
    ASSERT_TRUE(fsptr->FileExists(filePoolPath + "1000"));

    FilePoolOptions cspopt;
    cspopt.getFileFromPool = false;
    cspopt.fileSize = 16 * 1024;
    cspopt.metaPageSize = 4 * 1024;
    cspopt.metaFileSize = 4 * 1024;
    cspopt.retryTimes = 5;
    strcpy(cspopt.filePoolDir, filePoolPath.c_str());  // NOLINT

    chunkFilePoolPtr_ = std::make_shared<FilePool>(fsptr);
    ASSERT_TRUE(chunkFilePoolPtr_);
    ASSERT_TRUE(chunkFilePoolPtr_->Initialize(cspopt));
    ASSERT_EQ(0, chunkFilePoolPtr_->Size());

    //Test to obtain chunk, chunkfile pool size remains unchanged and remains at 0
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

    //Test recycling chunk, file deleted, FilePool Size not affected
    chunkFilePoolPtr_->RecycleFile("./new1");
    ASSERT_EQ(0, chunkFilePoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists("./new1"));

    //Delete test files and directories
    ASSERT_EQ(0, fsptr->Close(fd));
    ASSERT_EQ(0, fsptr->Delete(filePoolPath + "1000"));
    ASSERT_EQ(0, fsptr->Delete(filePoolPath));
    chunkFilePoolPtr_->UnInitialize();
}
