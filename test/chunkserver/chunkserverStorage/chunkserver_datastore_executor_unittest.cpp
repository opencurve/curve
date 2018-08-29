/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:52:18 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */


#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <memory>

#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore_executor.h"

using curve::chunkserver::CSDataStore;
using curve::chunkserver::CSSfsAdaptor;
using curve::chunkserver::ChunkserverStorage;
using curve::chunkserver::CSDataStoreExecutor;

class CSDataStoreExecutor_test : public testing::Test{
 public:
        void SetUp() {
            std::string uri = "local:///home/";
            curve::chunkserver::ChunkserverStorage::Init();
            csSfsAdaptorPtr_ = curve::chunkserver::ChunkserverStorage::CreateFsAdaptor("" , uri);
        }

        void TearDown() {
            csSfsAdaptorPtr_ = nullptr;
        }
    std::shared_ptr<CSDataStoreExecutor>  CSDataStoreExecutorptr;
    std::shared_ptr<CSSfsAdaptor>  csSfsAdaptorPtr_;
};

TEST_F(CSDataStoreExecutor_test, ReadWriteChunk) {
    std::string filepath("/home/test.txt");
    CSDataStoreExecutorptr = std::shared_ptr<CSDataStoreExecutor>(new CSDataStoreExecutor(csSfsAdaptorPtr_, filepath));
    ASSERT_TRUE(CSDataStoreExecutorptr != nullptr);

    char writebuf[1024];
    memset(writebuf, 'a', 1024);
    writebuf[1023] = '\0';
    ASSERT_EQ(1, CSDataStoreExecutorptr->WriteChunk(writebuf, 0, 1024));

    size_t length = 1024;
    char readbuf[1024];
    memset(readbuf, 'b', 1024);
    ASSERT_EQ(1, CSDataStoreExecutorptr->ReadChunk(readbuf, 0, &length));
    readbuf[1023] = '\0';
    ASSERT_EQ(0, strcmp(readbuf, writebuf));
    ASSERT_TRUE(CSDataStoreExecutorptr->DeleteChunk());
}

TEST_F(CSDataStoreExecutor_test, DeleteChunk) {
    std::string filepath = "/home/test.txt";
    CSDataStoreExecutorptr = std::shared_ptr<CSDataStoreExecutor>(new CSDataStoreExecutor(csSfsAdaptorPtr_, filepath));
    ASSERT_TRUE(CSDataStoreExecutorptr != nullptr);

    char writebuf[1024];
    memset(writebuf, 'a', 1024);
    writebuf[1023] = '\0';
    ASSERT_EQ(1, CSDataStoreExecutorptr->WriteChunk(writebuf, 0, 1024));
    ASSERT_TRUE(CSDataStoreExecutorptr->DeleteChunk());
}
