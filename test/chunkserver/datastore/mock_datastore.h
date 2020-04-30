/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_DATASTORE_MOCK_DATASTORE_H_
#define TEST_CHUNKSERVER_DATASTORE_MOCK_DATASTORE_H_

#include <gmock/gmock.h>
#include <string>

#include "src/chunkserver/datastore/chunkserver_datastore.h"

namespace curve {
namespace chunkserver {

class MockDataStore : public CSDataStore {
 public:
    MockDataStore() = default;
    ~MockDataStore() = default;
    MOCK_METHOD0(Initialize, bool());
    MOCK_METHOD2(DeleteChunk, CSErrorCode(ChunkID, SequenceNum));
    MOCK_METHOD2(DeleteSnapshotChunkOrCorrectSn, CSErrorCode(ChunkID,
                                                             SequenceNum));
    MOCK_METHOD5(ReadChunk, CSErrorCode(ChunkID,
                                        SequenceNum,
                                        char*,
                                        off_t,
                                        size_t));
    MOCK_METHOD5(ReadSnapshotChunk, CSErrorCode(ChunkID,
                                                SequenceNum,
                                                char*,
                                                off_t,
                                                size_t));
    MOCK_METHOD7(WriteChunk, CSErrorCode(ChunkID,
                                         SequenceNum,
                                         const char*,
                                         off_t,
                                         size_t,
                                         uint32_t*,
                                         const string&));
    MOCK_METHOD5(CreateCloneChunk, CSErrorCode(ChunkID,
                                               SequenceNum,
                                               SequenceNum,
                                               ChunkSizeType,
                                               const string&));
    MOCK_METHOD4(PasteChunk, CSErrorCode(ChunkID,
                                         const char*,
                                         off_t,
                                         size_t));
    MOCK_METHOD2(GetChunkInfo, CSErrorCode(ChunkID, CSChunkInfo*));
    MOCK_METHOD0(GetStatus, DataStoreStatus());
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_DATASTORE_MOCK_DATASTORE_H_
