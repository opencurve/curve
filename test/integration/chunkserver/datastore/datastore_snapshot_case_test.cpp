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

const string baseDir = "./data_int_sna";                     // NOLINT
const string poolDir = "./chunkfilepool_int_sna";            // NOLINT
const string poolMetaPath = "./chunkfilepool_int_sna.meta";  // NOLINT

class SnapshotTestSuit : public DatastoreIntegrationBase {
 public:
    SnapshotTestSuit() {}
    ~SnapshotTestSuit() {}
};

/**
 * Snapshot scenario testing
 * Construct a file with two chunks, chunk1 and chunk2, as follows
 * 1. Write chunk1
 * 2. Simulate the first snapshot taken, write chunk1 during the dump process
 * and generate a snapshot, but chunk2 does not have data write
 * 3. Delete the snapshot and write data to chunk2
 * 4. Simulate taking a second snapshot, writing chunk1 during the dump process,
 * but not chunk2
 * 5. Delete the snapshot and write data to chunk2 again
 * 6. Delete files
 */
TEST_F(SnapshotTestSuit, SnapshotTest) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    ChunkID id1 = 1;
    ChunkID id2 = 2;
    CSChunkInfo chunk1Info;
    CSChunkInfo chunk2Info;

    /****************** Creating Initial Environment, Creating Chunk1
     * ******************/

    // Write data '1' to the [0, 12KB) area of chunk1
    offset = 0;
    length = 3 * PAGE_SIZE;  // 12KB
    char buf1_1[3 * PAGE_SIZE];
    memset(buf1_1, '1', length);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_1, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    /******************Scene 1: Take the first snapshot of the
     * file******************/

    // Simulate taking a snapshot, where the file version increases
    ++fileSn;  // fileSn == 2

    // Write data '2' to the [4KB, 8KB] area of chunk1
    offset = 1 * PAGE_SIZE;
    length = 1 * PAGE_SIZE;
    char buf1_2[3 * PAGE_SIZE];
    memset(buf1_2, '2', 3 * PAGE_SIZE);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Information on chunk1 can be obtained, and all information meets
    // expectations
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(1, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    size_t readSize = 3 * PAGE_SIZE;
    char readbuf[3 * PAGE_SIZE];
    // Read the [0, 12KB) area of the chunk1 snapshot file, and the data read
    // should all be '1'
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,    // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1, readbuf, readSize));

    // Repeat write, verify that there will be no duplicate rows, and when
    // reading the snapshot, the data in the [4KB, 8KB] area should be '1'
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Write to an uncooked area, write to the [0,4kb] area
    offset = 0;
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Write the area that has been partially cowed, and write the [4kb, 12kb]
    // area
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Information on chunk1 can be obtained, and all information meets
    // expectations
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, chunk1Info.curSn);
    ASSERT_EQ(1, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // At this point, the data content returned by reading chunk1 should be
    // [0,12KB]:2 The data content returned from reading chunk1 snapshot should
    // be [0, 12KB):1 The data in other address spaces can be guaranteed without
    // any need
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id1,  // chunk id
                                      fileSn, readbuf,
                                      0,  // offset
                                      readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_2, readbuf, readSize));

    // When reading the [0, 12KB) area of the chunk1 snapshot file, the read
    // data should still be '1'
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,    // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1, readbuf, readSize));

    // ReadSnapshotChun, request offset+length > page size
    offset = CHUNK_SIZE - PAGE_SIZE;
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,    // snap sn
                                              readbuf,
                                              offset,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::InvalidArgError);

    // Read chunk2 snapshot file and return ChunkNotExistError
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id2,  // chunk id
                                              1,    // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    /******************Scene 2: First snapshot completes, delete
     * snapshot******************/

    // Request to delete the snapshot of chunk1, return success, and delete the
    // snapshot
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Check chunk1 information, as expected
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(0, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // Request to delete the snapshot of chunk2, returned success
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // Write data 'a' to the [0, 8KB) area of chunk2
    offset = 0;
    length = 2 * PAGE_SIZE;  // 8KB
    char buf2_2[2 * PAGE_SIZE];
    memset(buf2_2, 'a', length);
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn, buf2_2, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Check chunk1 information, as expected
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(0, chunk2Info.correctedSn);

    /******************Scene 3: Take second snapshot******************/

    // Simulate taking a second snapshot and increasing the version
    ++fileSn;  // fileSn == 3

    // Write data '3' to the [0KB, 8KB) area of chunk1
    offset = 0;
    length = 2 * PAGE_SIZE;
    char buf1_3[2 * PAGE_SIZE];
    memset(buf1_3, '3', length);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn, buf1_3, offset, length, nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Information on chunk1 can be obtained, and all information meets
    // expectations
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(2, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // At this point, the data content returned by reading chunk1 should be
    // [0,8KB]:3, [8KB, 12KB]:2 The data content returned from reading chunk1
    // snapshot should be [0, 12KB]:2 The data in other address spaces can be
    // guaranteed without any need
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id1,  // chunk id
                                      fileSn, readbuf,
                                      0,  // offset
                                      readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_3, readbuf, 2 * PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_2, readbuf + 2 * PAGE_SIZE, 1 * PAGE_SIZE));

    // Read the [0, 12KB) area of the chunk1 snapshot file, with data content of
    // '2'
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              2,    // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_2, readbuf, readSize));

    // The data content returned by reading the chunk2 snapshot should be [0,
    // 8KB): a, and the data in the other address spaces can be guaranteed
    // without any need to
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id2,  // chunk id
                                              2,    // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2_2, readbuf, readSize));

    /******************Scene 4: Second snapshot completes, delete
     * snapshot******************/

    // Request to delete snapshot of chunk1, returned success
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Check chunk1 information, as expected
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(0, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // Request to delete the snapshot of chunk2, returned success
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // Check chunk2 information, as expected
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    // Write data 'b' to the [0KB, 4KB) area of chunk2
    offset = 0;
    length = 1 * PAGE_SIZE;
    char buf2_3[1 * PAGE_SIZE];
    memset(buf2_3, 'b', length);
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn, buf2_3, offset, length, nullptr);
    // Check chunk2 information, as expected, curSn becomes 3, no snapshot will
    // be generated
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    // Write data to the [0KB, 8KB) area of chunk2 again
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn, buf2_3, offset, length, nullptr);
    // Check chunk2 information, chunk information remains unchanged and no
    // snapshot will be generated
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    /******************Scene 5: User Deletes File******************/

    // At this point, delete Chunk1 and return to Success
    errorCode = dataStore_->DeleteChunk(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // At this point, delete Chunk2 and return to Success
    errorCode = dataStore_->DeleteChunk(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id2, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
}

}  // namespace chunkserver
}  // namespace curve
