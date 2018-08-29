/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:52:53 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <memory>

#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.h"

using curve::chunkserver::ChunkserverStorage;
using curve::chunkserver::CSSfsAdaptor;

class CSSfsLocalFsImpl_test : public testing::Test {
 public:
        void SetUp() {
            std::string uri = "local:///home/";
            curve::chunkserver::ChunkserverStorage::Init();
            csSfsAdaptorPtr_ = curve::chunkserver::ChunkserverStorage::CreateFsAdaptor("" , uri);
        }

        void TearDown() {
            csSfsAdaptorPtr_ = nullptr;
        }

    std::shared_ptr<CSSfsAdaptor>  csSfsAdaptorPtr_;
};

TEST_F(CSSfsLocalFsImpl_test, FsOpenClose) {
    CSSfsAdaptor::fd_t fd = csSfsAdaptorPtr_->Open("/home/test.txt", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_NE(-1 , fd.fd_);
    ASSERT_NE(-1, csSfsAdaptorPtr_->Close(fd));
}

TEST_F(CSSfsLocalFsImpl_test, FsReadWrite) {
    CSSfsAdaptor::fd_t fd = csSfsAdaptorPtr_->Open("/home/test.txt", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_NE(-1 , fd.fd_);
    for (uint64_t id = 0; id < 1000; id++) {
        char writebuf[1024];
        memset(writebuf, 'a', 1024);
        writebuf[1023] = '\0';
        ASSERT_TRUE(csSfsAdaptorPtr_->Write(fd, writebuf, 0, 1024));

        char readbuf[1024];
        memset(readbuf, 'b', 1024);
        ASSERT_EQ(1024, csSfsAdaptorPtr_->Read(fd, readbuf, 0, 1024));
        readbuf[1023] = '\0';
        ASSERT_EQ(0, strcmp(readbuf, writebuf));
    }
    ASSERT_EQ(0, csSfsAdaptorPtr_->Delete("/home/test.txt"));
}

TEST_F(CSSfsLocalFsImpl_test, FsMkDirAndDirExist) {
    std::string dirpath = "/home/testdir";
    ASSERT_FALSE(csSfsAdaptorPtr_->DirExists(dirpath.c_str()));
    ASSERT_EQ(0, csSfsAdaptorPtr_->Mkdir(dirpath.c_str(), 0777));
    ASSERT_TRUE(csSfsAdaptorPtr_->DirExists(dirpath.c_str()));
    ASSERT_EQ(0, csSfsAdaptorPtr_->Delete(dirpath.c_str()));
    ASSERT_FALSE(csSfsAdaptorPtr_->DirExists(dirpath.c_str()));
}

TEST_F(CSSfsLocalFsImpl_test, FsFileExistAndDelete) {
    std::string filepath = "/home/test.txt";
    CSSfsAdaptor::fd_t fd = csSfsAdaptorPtr_->Open(filepath.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_NE(-1 , fd.fd_);
    ASSERT_EQ(0, csSfsAdaptorPtr_->Delete(filepath.c_str()));
}
