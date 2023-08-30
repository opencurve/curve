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
 * Created Date: Saturday January 5th 2019
 * Author: yangyaokai
 */

#include "test/integration/chunkserver/datastore/datastore_integration_base.h"

namespace curve {
namespace chunkserver {

const string baseDir = "./data_int_clo";    // NOLINT
const string poolDir = "./chunkfilepool_int_clo";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_clo.meta";  // NOLINT

class CloneTestSuit : public DatastoreIntegrationBase {
 public:
    CloneTestSuit() {}
    ~CloneTestSuit() {}
};

/**
 *Clone scenario testing
 */
TEST_F(CloneTestSuit, CloneTest) {
    ChunkID id = 1;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo info;
    std::string location("test@s3");

    /******************Scenario 1: Creating Cloned Files******************/

    //Create clone file chunk1
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(id, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    //Call the interface again, but still return success. Chunk information remains unchanged
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(id, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    //Create clone file chunk2
    errorCode = dataStore_->CreateCloneChunk(2,  // chunk id
                                             sn,
                                             correctedSn,
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(2, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(2, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    /******************Scene 2: Restoring Cloned Files******************/
    //Construct raw data
    char pasteBuf[4 * PAGE_SIZE];
    memset(pasteBuf, '1', 4 * PAGE_SIZE);
    //WriteChunk writes data to the [0, 8KB] area of the clone chunk
    offset = 0;
    length = 2 * PAGE_SIZE;
    char writeBuf1[2 * PAGE_SIZE];
    memset(writeBuf1, 'a', length);
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       writeBuf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    //Reading Chunk data, [0, 8KB] data should be '1'
    size_t readSize = 2 * PAGE_SIZE;
    char readBuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, readSize));

    //PasteChunk writes data again to the [0, 8KB] area of the clone chunk
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    //Reading Chunk data, [0, 8KB] data should be 'a'
    readSize = 2 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, readSize));

    //WriteChunk writes data again to the [4KB, 12KB] area of the clone chunk
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    char writeBuf3[2 * PAGE_SIZE];
    memset(writeBuf3, 'c', length);
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       writeBuf3,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(3, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    //Reading Chunk data, [0, 4KB] data should be 'a', [4KB, 12KB] data should be 'c'
    readSize = 3 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(writeBuf3, readBuf + PAGE_SIZE, 2 * PAGE_SIZE));

    /******************Scene 3: Conversion of Cloned Files after Iterative Writing into Regular Chunk Files*************/

    char overBuf[1 * kMB] = {0};
    for (int i = 0; i < 16; ++i) {
        errorCode = dataStore_->PasteChunk(id,
                                           overBuf,
                                           i * kMB,   // offset
                                           1 * kMB);  // length
        ASSERT_EQ(errorCode, CSErrorCode::Success);
    }
    //Check all the information of the chunk and ensure it meets expectations. The chunk will be converted to a regular chunk
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);

    /******************Scene 3: Delete File****************/

    //At this point, delete Chunk1 and return to Success
    errorCode = dataStore_->DeleteChunk(1, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    //At this point, delete Chunk2 and return to Success
    errorCode = dataStore_->DeleteChunk(2, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(2, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
}

/**
 *Recovery scenario testing
 */
TEST_F(CloneTestSuit, RecoverTest) {
    ChunkID id = 1;
    SequenceNum sn = 2;
    SequenceNum correctedSn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo info;
    std::string location("test@s3");

    /******************Scenario 1: Creating Cloned Files******************/

    //Create clone file chunk1
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,  // corrected sn
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(id, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    //Call the interface again, but still return success. Chunk information remains unchanged
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             3,  // corrected sn
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(id, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    /******************Scene 2: Restoring Cloned Files******************/
    sn = 3;
    //Construct raw data
    char pasteBuf[4 * PAGE_SIZE];
    memset(pasteBuf, '1', 4 * PAGE_SIZE);
    //PasteChunk writes data to the [0, 8KB] area of the clone chunk
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    //Reading Chunk data, [0, 8KB] data should be '1'
    size_t readSize = 2 * PAGE_SIZE;
    char readBuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(pasteBuf, readBuf, readSize));

    //WriteChunk writes data again to the [0, 8KB] area of the clone chunk
    offset = 0;
    length = 2 * PAGE_SIZE;
    char writeBuf2[2 * PAGE_SIZE];
    memset(writeBuf2, 'b', length);
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       writeBuf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    //Reading Chunk data, [0, 8KB] data should be 'b'
    readSize = 2 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf2, readBuf, readSize));

    //PasteChunk writes data again to the [4KB, 12KB] area of the clone chunk
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check all the information of the chunk and ensure it meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    //Write the corresponding bit of PAGE to 1, and all other bits are set to 0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(3, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    //Reading Chunk data, [0, 8KB] data should be 'b', [8KB, 12KB] data should be '1'
    readSize = 3 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf2, readBuf, 2 * PAGE_SIZE));
    ASSERT_EQ(0, memcmp(pasteBuf, readBuf + 2 * PAGE_SIZE, PAGE_SIZE));

    /******************Scene 3: Convert Cloned Files from Sequential Write to Regular Chunk Files*************/

    char overBuf[1 * kMB] = {0};
    for (int i = 0; i < 16; ++i) {
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           overBuf,
                                           i * kMB,   // offset
                                           1 * kMB,
                                           nullptr);  // length
        ASSERT_EQ(errorCode, CSErrorCode::Success);
    }
    //Check all the information of the chunk and ensure it meets expectations. The chunk will be converted to a regular chunk
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);
}

}  // namespace chunkserver
}  // namespace curve
