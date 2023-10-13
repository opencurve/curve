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

const uint64_t kMB = 1024 * 1024;
const ChunkSizeType CHUNK_SIZE = 16 * kMB;
const PageSizeType PAGE_SIZE = 4 * 1024;
const string baseDir = "./data_int";                     // NOLINT
const string poolDir = "./chunkfilepool_int";            // NOLINT
const string poolMetaPath = "./chunkfilepool_int.meta";  // NOLINT

class DatastoreIntegrationTest : public DatastoreIntegrationBase {
 public:
    DatastoreIntegrationTest() {}
    ~DatastoreIntegrationTest() {}
};

/**
 * Basic functional testing verification
 * Read, write, delete, and obtain file information
 */
TEST_F(DatastoreIntegrationTest, BasicTest) {
    ChunkID id = 1;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    std::string chunkPath =
        baseDir + "/" + FileNameOperator::GenerateChunkFileName(id);
    CSErrorCode errorCode;
    CSChunkInfo info;

    /******************Scenario 1: New File Created, Chunk File Does Not
     * Exist******************/

    // File does not exist
    ASSERT_FALSE(lfs_->FileExists(chunkPath));

    // chunkNotExistError returned when reading chunk
    char readbuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // Unable to obtain the version number of the chunk
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // Delete chunk and return Success
    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    /******************Scene 2: Operations after generating chunk files via
     * WriteChunk.**************/

    char buf1_1_1[PAGE_SIZE];
    memset(buf1_1_1, 'a', length);
    // The first WriteChunk will generate a chunk file
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
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);

    // Verify that the 4KB read and written should be equal to the data written
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_1, readbuf, length));

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
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
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, 0, 3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_3, readbuf + PAGE_SIZE, PAGE_SIZE));

    // When a chunk exists, it covers some areas
    char buf1_1_4[2 * PAGE_SIZE];
    memset(buf1_1_4, 'd', length);
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode =
        dataStore_->WriteChunk(id, sn, buf1_1_4, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Areas that have not been written can also be read, but the data content
    // read is not guaranteed
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, 0, 3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_4, readbuf + PAGE_SIZE, 2 * PAGE_SIZE));

    /******************Scene 3: User Deletes File******************/

    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
    ASSERT_FALSE(lfs_->FileExists(chunkPath));
}

/**
 * Restart Recovery Test
 */
TEST_F(DatastoreIntegrationTest, RestartTest) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSChunkInfo info1;
    CSChunkInfo info2;
    CSChunkInfo info3;
    std::string location("test@s3");

    // Construct read and write buffers to be used
    char buf1_1[2 * PAGE_SIZE];
    memset(buf1_1, 'a', length);
    char buf2_1[2 * PAGE_SIZE];
    memset(buf2_1, 'a', length);
    char buf2_2[2 * PAGE_SIZE];
    memset(buf2_2, 'b', length);
    char buf2_4[2 * PAGE_SIZE];
    memset(buf2_4, 'c', length);
    char writeBuf[2 * PAGE_SIZE];
    memset(writeBuf, '1', length);
    char pasteBuf[2 * PAGE_SIZE];
    memset(pasteBuf, '2', length);
    size_t readSize = 4 * PAGE_SIZE;
    char readBuf[4 * PAGE_SIZE];

    // The error code return value corresponding to each operation, and the
    // error code naming format is e_optype_chunid_sn
    CSErrorCode e_write_1_1;
    CSErrorCode e_write_2_1;
    CSErrorCode e_write_2_2;
    CSErrorCode e_write_2_4;
    CSErrorCode e_write_3_1;
    CSErrorCode e_paste_3_1;
    CSErrorCode e_del_1_1;
    CSErrorCode e_delsnap_2_2;
    CSErrorCode e_delsnap_2_3;
    CSErrorCode e_clone_3_1;

    // Simulate all user requests and use the lamdba function to validate the
    // reuse of this code during log recovery If you want to add use cases
    // later, you only need to add operations within the function
    auto ApplyRequests = [&]() {
        fileSn = 1;
        // Simulate ordinary file operations, WriteChunk generates chunk1,
        // chunk2
        offset = 0;
        length = 2 * PAGE_SIZE;
        // Generate chunk1
        e_write_1_1 =
            dataStore_->WriteChunk(1,  // chunk id
                                   fileSn, buf1_1, offset, length, nullptr);
        // Generate chunk2
        e_write_2_1 =
            dataStore_->WriteChunk(2,  // chunk id
                                   fileSn, buf1_1, offset, length, nullptr);
        // Delete chunk1
        e_del_1_1 = dataStore_->DeleteChunk(1, fileSn);

        // Simulate snapshot operations
        ++fileSn;
        offset = 1 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        // Write chunk2 to generate a snapshot file
        e_write_2_2 =
            dataStore_->WriteChunk(2,  // chunk id
                                   fileSn, buf2_2, offset, length, nullptr);
        // Delete chunk2 snapshot
        e_delsnap_2_2 = dataStore_->DeleteSnapshotChunkOrCorrectSn(2, fileSn);
        // Simulate taking another snapshot and then delete the chunk2 snapshot
        ++fileSn;
        e_delsnap_2_3 = dataStore_->DeleteSnapshotChunkOrCorrectSn(2, fileSn);
        // Simulate another snapshot, then write data to chunk2 to generate a
        // snapshot
        ++fileSn;
        offset = 2 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        // Write chunk2 to generate a snapshot file
        e_write_2_4 =
            dataStore_->WriteChunk(2,  // chunk id
                                   fileSn, buf2_4, offset, length, nullptr);

        // Simulate Clone Operations
        e_clone_3_1 = dataStore_->CreateCloneChunk(3,  // chunk id
                                                   1,  // sn
                                                   0,  // corrected sn
                                                   CHUNK_SIZE, location);
        // Write data to chunk3
        offset = 0;
        length = 2 * PAGE_SIZE;
        // Write chunk3
        e_write_3_1 = dataStore_->WriteChunk(3,  // chunk id
                                             1,  // sn
                                             writeBuf, offset, length, nullptr);
        // Paste data to chunk3
        offset = 1 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        e_paste_3_1 = dataStore_->PasteChunk(3,  // chunk id
                                             pasteBuf, offset, length);
    };

    // After checking the user actions above, the status of each file in the
    // DataStore layer can be reused
    auto CheckStatus = [&]() {
        CSErrorCode errorCode;
        // Chunk1 does not exist
        errorCode = dataStore_->GetChunkInfo(1, &info1);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
        // Chunk2 exists, version 4, correctedSn is 3, snapshot exists, snapshot
        // version 2
        errorCode = dataStore_->GetChunkInfo(2, &info2);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(4, info2.curSn);
        ASSERT_EQ(2, info2.snapSn);
        ASSERT_EQ(3, info2.correctedSn);
        // Check chunk2 data, [0, 1KB]:a , [1KB, 2KB]:b , [2KB, 4KB]:c
        errorCode = dataStore_->ReadChunk(2, fileSn, readBuf, 0, readSize);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf2_1, readBuf, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_2, readBuf + 1 * PAGE_SIZE, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_4, readBuf + 2 * PAGE_SIZE, 2 * PAGE_SIZE));
        // Check chunk2 snapshot data, [0, 1KB]:a , [1KB, 3KB]:b
        errorCode = dataStore_->ReadSnapshotChunk(2, 2, readBuf, 0, readSize);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf2_1, readBuf, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_2, readBuf + 1 * PAGE_SIZE, 2 * PAGE_SIZE));
    };

    /******************Generate data before reboot******************/
    // Submit Action
    ApplyRequests();
    // Check if the return value of each operation meets expectations
    ASSERT_EQ(e_write_1_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_1, CSErrorCode::Success);
    ASSERT_EQ(e_del_1_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_2, CSErrorCode::Success);
    ASSERT_EQ(e_delsnap_2_2, CSErrorCode::Success);
    ASSERT_EQ(e_delsnap_2_3, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_4, CSErrorCode::Success);
    ASSERT_EQ(e_clone_3_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_3_1, CSErrorCode::Success);
    ASSERT_EQ(e_paste_3_1, CSErrorCode::Success);
    // Check the status of each file at this time
    CheckStatus();

    /******************Scene 1: Reboot and Reload Files******************/
    // Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.pageSize = PAGE_SIZE;
    // Construct a new dataStore_, And reinitialize
    dataStore_ = std::make_shared<CSDataStore>(lfs_, filePool_, options);
    ASSERT_TRUE(dataStore_->Initialize());
    // Check the status of each chunk, which should be consistent with the
    // previous one
    CheckStatus();

    /******************Scene 2: Restore logs, replay previous
     * actions******************/
    // Simulate log playback
    ApplyRequests();
    // Check if the return value of each operation meets expectations
    ASSERT_EQ(e_write_1_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_1, CSErrorCode::BackwardRequestError);
    ASSERT_EQ(e_del_1_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_2, CSErrorCode::BackwardRequestError);
    ASSERT_EQ(e_delsnap_2_2, CSErrorCode::Success);
    ASSERT_EQ(e_delsnap_2_3, CSErrorCode::Success);
    ASSERT_EQ(e_write_2_4, CSErrorCode::Success);
    ASSERT_EQ(e_clone_3_1, CSErrorCode::Success);
    ASSERT_EQ(e_write_3_1, CSErrorCode::Success);
    ASSERT_EQ(e_paste_3_1, CSErrorCode::Success);
}

}  // namespace chunkserver
}  // namespace curve
