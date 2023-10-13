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

const string baseDir = "./data_int_bas";                     // NOLINT
const string poolDir = "./chunkfilepool_int_bas";            // NOLINT
const string poolMetaPath = "./chunkfilepool_int_bas.meta";  // NOLINT

class BasicTestSuit : public DatastoreIntegrationBase {
 public:
    BasicTestSuit() {}
    ~BasicTestSuit() {}
};

/**
 * Basic functional testing verification
 * Read, write, delete, and obtain file information
 */
TEST_F(BasicTestSuit, BasicTest) {
    ChunkID id = 1;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    std::string chunkPath =
        baseDir + "/" + FileNameOperator::GenerateChunkFileName(id);
    CSErrorCode errorCode;
    CSChunkInfo info;

    /******************Scene One: New file created, Chunk file does not
     * exist******************/

    // File does not exist
    ASSERT_FALSE(lfs_->FileExists(chunkPath));

    // ChunkNotExistError returned when reading chunk
    char readbuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // Unable to obtain the version number of the chunk
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // Delete chunk and return Success
    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    /****************** Scene Two: Operations after generating chunk files
     * through WriteChunk **************/

    char buf1_1_1[PAGE_SIZE];
    memset(buf1_1_1, 'a', length);

    errorCode =
        dataStore_->WriteChunk(id, sn, buf1_1_1, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Chunk information can be obtained and all information meets expectations
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    ASSERT_EQ(id, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);

    // Verify that the 4KB read and written should be equal to the data written
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_1, readbuf, length));

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
    memset(readbuf, 0, sizeof(readbuf));
    errorCode =
        dataStore_->ReadChunk(id, sn, readbuf, CHUNK_SIZE - PAGE_SIZE, length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Overwrite when chunk exists
    char buf1_1_2[PAGE_SIZE];
    memset(buf1_1_2, 'b', length);

    errorCode =
        dataStore_->WriteChunk(id, sn, buf1_1_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, 3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, length));

    // When a chunk exists, write to an unwritten area
    char buf1_1_3[PAGE_SIZE];
    memset(buf1_1_3, 'c', length);
    offset = PAGE_SIZE;
    length = PAGE_SIZE;

    errorCode =
        dataStore_->WriteChunk(id, sn, buf1_1_3, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, 0, 3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_3, readbuf + PAGE_SIZE, PAGE_SIZE));

    // When a chunk exists, it covers some areas
    char buf1_1_4[2 * PAGE_SIZE];
    memset(buf1_1_4, 'd', length);
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;

    butil::IOBuf iobuf1_1_4;
    iobuf1_1_4.append(buf1_1_4, length);

    errorCode =
        dataStore_->WriteChunk(id, sn, iobuf1_1_4, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, 0, 3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_4, readbuf + PAGE_SIZE, 2 * PAGE_SIZE));

    /******************Scene Three: User deletes file******************/

    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
    ASSERT_FALSE(lfs_->FileExists(chunkPath));
}

}  // namespace chunkserver
}  // namespace curve
