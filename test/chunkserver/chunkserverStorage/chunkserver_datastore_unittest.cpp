/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:51:56 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"

using curve::chunkserver::ChunkserverStorage;
using curve::chunkserver::CSSfsAdaptor;
using curve::chunkserver::CSDataStore;

class CSDataStore_test : public testing::Test {
 public:
        void SetUp() {
            std::string uri = "local://./";
            curve::chunkserver::ChunkserverStorage::Init();
            testsfsada_ =
            curve::chunkserver::ChunkserverStorage::CreateFsAdaptor("" , uri);
        }

        void TearDown() {
            testsfsada_ = nullptr;
        }

    std::shared_ptr<CSSfsAdaptor> testsfsada_;
    std::shared_ptr<CSDataStore>  cpmana;
};

TEST_F(CSDataStore_test, ReadWriteChunk) {
    std::string datastorepath = "./1/2/3/test1//";
    cpmana = std::shared_ptr<CSDataStore>(
        new CSDataStore());
    ASSERT_TRUE(cpmana->Initialize(testsfsada_, datastorepath));

    for (uint64_t chunkid = 0; chunkid < 1000; chunkid++) {
        char writebuf[1024];
        memset(writebuf, 'a', 1024);
        writebuf[1023] = '\0';
        ASSERT_EQ(1, cpmana->WriteChunk(chunkid, writebuf, 0, 1024));

        size_t length = 1024;
        char readbuf[1024];
        memset(readbuf, 'b', 1024);
        ASSERT_EQ(1, cpmana->ReadChunk(chunkid, readbuf, 0, &length));
        readbuf[1023] = '\0';
        ASSERT_EQ(0, strcmp(readbuf, writebuf));
        ASSERT_TRUE(cpmana->DeleteChunk(chunkid));
    }
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/test1/data"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/test1/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1"));
}

TEST_F(CSDataStore_test, DeleteChunk) {
    std::string datastorepath = "./1/2/3/test1//";
    cpmana = std::shared_ptr<CSDataStore>(
        new CSDataStore());
    ASSERT_TRUE(cpmana->Initialize(testsfsada_, datastorepath));

    curve::chunkserver::ChunkID chunkid = 1;
    char writebuf[1024];
    memset(writebuf, 'a', 1024);
    ASSERT_EQ(1, cpmana->WriteChunk(chunkid, writebuf, 0, 1024));

    ASSERT_TRUE(cpmana->DeleteChunk(chunkid));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/test1/data"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/test1/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/3/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1/2/"));
    ASSERT_TRUE(testsfsada_->Rmdir("./1"));
}
