/*
 * Project: curve
 * File Created: Monday, 10th December 2018 3:22:12 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <climits>
#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::chunkserver::ChunkfilePool;
using curve::chunkserver::ChunkfilePoolOptions;

class CSChunkfilePool_test : public testing::Test {
 public:
    void SetUp() {
        fsptr = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        ChunkfilepoolPtr_ = std::make_shared<ChunkfilePool>(fsptr);
        if (ChunkfilepoolPtr_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        int count = 1;
        std::string dirname = "./chunkfilepool";
        while (count < 3) {
            std::string  filename = "./chunkfilepool/" + std::to_string(count);
            fsptr->Mkdir("./chunkfilepool");
            int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
            char data[8192];
            memset(data, 'a', 8192);
            fsptr->Write(fd, data, 0, 8192);
            fsptr->Close(fd);
            count++;
        }
        // persistency: chunksize, metapagesize, chunkfilepool path, allocate percent                                   // NOLINT
        char persistency[4096] = {0};
        uint32_t chunksize = 4096;
        uint32_t metapagesize = 4096;
        uint32_t percent = 10;
        ::memcpy(persistency, &chunksize, sizeof(uint32_t));
        ::memcpy(persistency + sizeof(uint32_t), &metapagesize, sizeof(uint32_t));      // NOLINT
        ::memcpy(persistency + 2*sizeof(uint32_t), &percent, sizeof(uint32_t));
        ::memcpy(persistency + 3*sizeof(uint32_t), dirname.c_str(), dirname.size());    // NOLINT

        int fd = fsptr->Open("./chunkfilepool.meta", O_RDWR | O_CREAT);
        if (fd < 0) {
            return;
        }
        int ret = fsptr->Write(fd, persistency, 0, 4096);
        if (ret != 4096) {
            return;
        }
        fsptr->Close(fd);

        fd = fsptr->Open("./chunkfilepool.meta2", O_RDWR | O_CREAT);
        if (fd < 0) {
            return;
        }
        ret = fsptr->Write(fd, persistency, 0, 2048);
        if (ret != 2048) {
            return;
        }
        fsptr->Close(fd);
    }

    void TearDown() {
        std::vector<std::string> filename;
        fsptr->List("./chunkfilepool", &filename);
        for (auto iter : filename) {
            auto path = "./chunkfilepool/" + iter;
            int err = fsptr->Delete(path.c_str());
            if (err) {
                LOG(INFO) << "unlink file failed!, errno = " << errno;
            }
        }
        fsptr->Delete("./chunkfilepool");
        fsptr->Delete("./chunkfilepool.meta");
        fsptr->Delete("./chunkfilepool.meta2");
        ChunkfilepoolPtr_->UnInitialize();
    }

    std::shared_ptr<ChunkfilePool>  ChunkfilepoolPtr_;
    std::shared_ptr<LocalFileSystem>  fsptr;
};

TEST_F(CSChunkfilePool_test, InitializeTest) {
    std::string chunkfilepool = "./chunkfilepool.meta";

    ChunkfilePoolOptions cfop;
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    memcpy(cfop.metapath, chunkfilepool.c_str(), chunkfilepool.size());

    // test meta content wrong
    ASSERT_TRUE(ChunkfilepoolPtr_->Initialize(cfop));
    ASSERT_EQ(2, ChunkfilepoolPtr_->Size());
    cfop.chunksize = 8192;
    cfop.metapagesize = 4096;
    // test meta content wrong
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    cfop.chunksize = 8192;
    cfop.metapagesize = 4096;
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // invalid file name
    std::string  filename = "./chunkfilepool/a";
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
    char data[8192];
    memset(data, 'a', 8192);
    fsptr->Write(fd, data, 0, 8192);
    fsptr->Close(fd);
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // test meta file wrong
    chunkfilepool = "./chunkfilepool.meta2";
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    memcpy(cfop.metapath, chunkfilepool.c_str(), chunkfilepool.size());
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));
    // test meta file not exist
    chunkfilepool = "./chunkfilepool.meta3";
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    memcpy(cfop.metapath, chunkfilepool.c_str(), chunkfilepool.size());
    ASSERT_FALSE(ChunkfilepoolPtr_->Initialize(cfop));

    fsptr->Delete("./chunkfilepool/a");
    fsptr->Delete("./chunkfilepool.meta3");
}

TEST_F(CSChunkfilePool_test, GetChunkTest) {
    std::string chunkfilepool = "./chunkfilepool.meta";
    ChunkfilePoolOptions cfop;
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    memcpy(cfop.metapath, chunkfilepool.c_str(), chunkfilepool.size());
    // test get chunk success
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new_exit", metapage));
    ASSERT_EQ(-2, fsptr->Delete("./new_exit"));
    ChunkfilepoolPtr_->Initialize(cfop);
    ASSERT_EQ(2, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_EQ(1, ChunkfilepoolPtr_->Size());
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
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new2", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new2"));
    ASSERT_NE(1, ChunkfilepoolPtr_->Size());
    ASSERT_EQ(0, fsptr->Delete("./new2"));

    // test get chunk fail
    ASSERT_EQ(-1, ChunkfilepoolPtr_->GetChunk("./new3", metapage));
}

TEST_F(CSChunkfilePool_test, RecycleChunkTest) {
    std::string chunkfilepool = "./chunkfilepool.meta";
    ChunkfilePoolOptions cfop;
    cfop.chunksize = 4096;
    cfop.metapagesize = 4096;
    memcpy(cfop.metapath, chunkfilepool.c_str(), chunkfilepool.size());

    ChunkfilepoolPtr_->Initialize(cfop);
    ASSERT_EQ(2, ChunkfilepoolPtr_->Size());
    char metapage[4096];
    memset(metapage, '1', 4096);
    ASSERT_EQ(0, ChunkfilepoolPtr_->GetChunk("./new1", metapage));
    ASSERT_TRUE(fsptr->FileExists("./new1"));
    ASSERT_EQ(1, ChunkfilepoolPtr_->Size());

    ChunkfilepoolPtr_->RecycleChunk("./new1");
    ASSERT_EQ(2, ChunkfilepoolPtr_->Size());
    ASSERT_FALSE(fsptr->FileExists("./new1"));
    ASSERT_TRUE(fsptr->FileExists("./chunkfilepool/4"));
    ASSERT_EQ(0, fsptr->Delete("./chunkfilepool/4"));
}
