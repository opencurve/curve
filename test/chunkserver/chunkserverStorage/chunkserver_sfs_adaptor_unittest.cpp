/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:52:36 am
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

class CSSfAdaptor_test : public testing::Test {
 public:
        void SetUp() {
            std::string uri = "local://./";
            curve::chunkserver::ChunkserverStorage::Init();
            csSfsAdaptorPtr_ =
            curve::chunkserver::ChunkserverStorage::CreateFsAdaptor("" , uri);
        }

        void TearDown() {
            csSfsAdaptorPtr_ = nullptr;
        }

    std::shared_ptr<CSSfsAdaptor>  csSfsAdaptorPtr_;
};

TEST_F(CSSfAdaptor_test, FsOpenClose) {
    CSSfsAdaptor::fd_t fd = csSfsAdaptorPtr_->
        Open("./test.txt", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_TRUE(fd.Valid());
    ASSERT_NE(-1, csSfsAdaptorPtr_->Close(fd));
}

TEST_F(CSSfAdaptor_test, FsReadWrite) {
    CSSfsAdaptor::fd_t fd = csSfsAdaptorPtr_->
        Open("./test.txt", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_TRUE(fd.Valid());
    for (uint64_t id = 0; id < 1000; id++) {
        char writebuf[1024];
        memset(writebuf, 'a', 1024);
        writebuf[1023] = '\0';
        csSfsAdaptorPtr_->Write(fd, writebuf, 0, 1024);

        size_t length = 1024;
        char readbuf[1024];
        memset(readbuf, 'b', 1024);
        csSfsAdaptorPtr_->Read(fd, readbuf, 0, length);
        readbuf[1023] = '\0';
        ASSERT_EQ(0, strcmp(readbuf, writebuf));
    }
    ASSERT_EQ(0, csSfsAdaptorPtr_->Delete("./test.txt"));
}
