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

const string baseDir = "./data_int_exc";    // NOLINT
const string poolDir = "./chunkfilepool_int_exc";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_exc.meta";  // NOLINT

class ExceptionTestSuit : public DatastoreIntegrationBase {
 public:
    ExceptionTestSuit() {}
    ~ExceptionTestSuit() {}
};

/**
 *Abnormal test 1
 *Use case: Chunk's metapage data is corrupt, and then start DataStore
 *Expected: Reboot failed
 */
TEST_F(ExceptionTestSuit, ExceptionTest1) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Modifying the metapage of chunk1 through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    //Modify Metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 *Abnormal Test 2
 *Use case: Chunk's metapage data is corrupt, then the metapage is updated, and then the DataStore is restarted
 *Expected: Restarting the datastore can be successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest2) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Modifying the metapage of chunk1 through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    //Modify Metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Trigger Metapage Update
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());
}

/**
 *Abnormal Test 3
 *Use case: Chunk snapshot metadata data corruption, then restart Datastore
 *Expected: Reboot failed
 */
TEST_F(ExceptionTestSuit, ExceptionTest3) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Generate snapshot files
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Modifying the metapage of chunk1 snapshot through lfs
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    //Modify Metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 *Abnormal Test 4
 *Use case: Chunk snapshot's metapage data is corrupt, but the metapage is updated, and then restart the Datastore
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest4) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Generate snapshot files
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Trigger snapshot metadata update
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset + PAGE_SIZE,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Modifying the metapage of chunk1 snapshot through lfs
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    //Modify Metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 *Abnormal Test 5
 *Use case: WriteChunk data is written halfway and restarted
 *Expected: Successful restart, successful re execution of the previous operation
 */
TEST_F(ExceptionTestSuit, ExceptionTest5) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Construct data to be written and request offset
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', length);
    offset = 0;
    length = 2 * PAGE_SIZE;
    //Write half of the data to the chunk file through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    //Write data
    lfs_->Write(fd, buf2, offset + PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Simulate log recovery
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Read data verification
    char readbuf[2 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
}

/**
 *Abnormal Test 6
 *Use case: WriteChunk updates the metapage and restarts, sn>chunk. sn, sn==chunk. correctedSn
 *Expected: Successful restart, successful re execution of the previous operation
 */
TEST_F(ExceptionTestSuit, ExceptionTest6) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Update correctedsn to 2
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(1, 2);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Construct request parameters to write
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', length);
    offset = 0;
    length = 2 * PAGE_SIZE;
    fileSn = 2;  // sn > chunk.sn; sn == chunk.correctedSn

    //Modifying the metapage of chunk1 through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metabuf[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);

    //Successfully simulated updating of metapage
    ChunkFileMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, metaPage.sn);
    metaPage.sn = fileSn;
    metaPage.encode(metabuf);
    //Update Metapage
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Simulate log recovery
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Read data verification
    char readbuf[2 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
}

/**
 *Abnormal Test 7
 *Use case: WriteChunk generates a snapshot and restarts, restoring historical and current operations
 *      sn>chunk.sn, sn>chunk.correctedSn
 *chunk.sn>chunk.correctedSn
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest7) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1 and simulate the situation where chunk.sn>chunk.correctedSn
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate creating snapshot files
    ChunkOptions chunkOption;
    chunkOption.id = 1;
    chunkOption.sn = 1;
    chunkOption.baseDir = baseDir;
    chunkOption.chunkSize = CHUNK_SIZE;
    chunkOption.blockSize = BLOCK_SIZE;
    chunkOption.metaPageSize = PAGE_SIZE;
    CSSnapshot snapshot(lfs_, filePool_, chunkOption);
    errorCode = snapshot.Open(true);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Check if snapshot information is loaded
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    //Simulate the previous operation of log recovery
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Read the snapshot file to verify if there is a row
    char readbuf[2 * PAGE_SIZE];
    snapshot.Read(readbuf, offset, length);
    //Expected no cows to occur
    ASSERT_NE(0, memcmp(buf1, readbuf, length));

    //Simulate recovery of the last operation
    fileSn++;
    char buf2[PAGE_SIZE];
    memset(buf2, '2', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check if the version number has been updated
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    //Chunk data is overwritten
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    //Raw data cow to snapshot
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 *Abnormal Test8
 *Use case: WriteChunk generates a snapshot and restarts,
 *      sn>chunk.sn, sn>chunk.correctedSn
 *      Test chunk.sn==chunk.correctedSn
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest8) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1 and construct a scenario where chunk.sn==chunk.correctedsn
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(1, 2);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate creating snapshot files
    ChunkOptions chunkOption;
    chunkOption.id = 1;
    chunkOption.sn = 2;
    chunkOption.baseDir = baseDir;
    chunkOption.chunkSize = CHUNK_SIZE;
    chunkOption.metaPageSize = PAGE_SIZE;
    chunkOption.blockSize = BLOCK_SIZE;
    CSSnapshot snapshot(lfs_, filePool_, chunkOption);
    errorCode = snapshot.Open(true);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Check if snapshot information is loaded
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(2, info.snapSn);
    ASSERT_EQ(2, info.correctedSn);

    //Simulate the previous operation of log recovery
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Read the snapshot file to verify if there is a row
    char readbuf[2 * PAGE_SIZE];
    snapshot.Read(readbuf, offset, length);
    //Expected no cows to occur
    ASSERT_NE(0, memcmp(buf1, readbuf, length));

    //Simulate recovery of the last operation
    fileSn++;
    char buf2[PAGE_SIZE];
    memset(buf2, '2', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check if the version number has been updated
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);
    ASSERT_EQ(2, info.correctedSn);
    //Chunk data is overwritten
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    //Raw data cow to snapshot
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              2,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 *Abnormal Test 9
 *Use case: WriteChunk generates a snapshot and updates the metapage before restarting, restoring historical and current operations
 *      sn>chunk.sn, sn>chunk.correctedSn
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest9) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1 and simulate the situation where chunk.sn>chunk.correctedSn
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate creating snapshot files
    ChunkOptions chunkOption;
    chunkOption.id = 1;
    chunkOption.sn = 1;
    chunkOption.baseDir = baseDir;
    chunkOption.chunkSize = CHUNK_SIZE;
    chunkOption.metaPageSize = PAGE_SIZE;
    chunkOption.blockSize = BLOCK_SIZE;
    CSSnapshot snapshot(lfs_, filePool_, chunkOption);
    errorCode = snapshot.Open(true);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Modifying the metapage of chunk1 through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metabuf[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);

    //Successfully simulated updating of metapage
    ChunkFileMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, metaPage.sn);
    metaPage.sn = 2;
    metaPage.encode(metabuf);
    //Update Metapage
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Check if snapshot information is loaded
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    //Simulate the previous operation of log recovery
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);

    //Simulate recovery of the last operation
    fileSn++;
    char buf2[PAGE_SIZE];
    memset(buf2, '2', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check if the version number has been updated
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    //Chunk data is overwritten
    char readbuf[2 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    //Raw data cow to snapshot
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 *Abnormal Test 10
 *Use case: WriteChunk restarts before updating the snapshot metapage to restore historical and current operations
 *      sn>chunk.sn, sn>chunk.correctedSn
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest10) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = 2 * PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1 and simulate the situation where chunk.sn>chunk.correctedSn
    char buf1[2 * PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Generate snapshot file
    fileSn++;
    length = PAGE_SIZE;
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', 2 * PAGE_SIZE);

    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate Cow
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    //Write data
    lfs_->Write(fd, buf1, 2 * PAGE_SIZE, PAGE_SIZE);
    //Update Metapage
    char metabuf[PAGE_SIZE];
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);
    //Modify Metapage
    SnapshotMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    metaPage.bitmap->Set(1);
    metaPage.encode(metabuf);
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Check if snapshot information is loaded
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    //Simulate log recovery
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       1,  // sn
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);
    //Simulate recovery of the next operation
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate recovery of the last operation
    offset = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check if the chunk information is correct
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    //Chunk data is overwritten
    char readbuf[2 * PAGE_SIZE];
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    //Raw data cow to snapshot
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 *Abnormal Test 11
 *Use case: WriteChunk updates snapshot metadata and restarts to restore historical and current operations
 *      sn>chunk.sn, sn>chunk.correctedSn
 *Expected: Reboot successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest11) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = 2 * PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    //Generate chunk1 and simulate the situation where chunk.sn>chunk.correctedSn
    char buf1[2 * PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Generate snapshot file
    fileSn++;
    length = PAGE_SIZE;
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', 2 * PAGE_SIZE);

    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate Cow
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    //Write data
    lfs_->Write(fd, buf1, 2 * PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Check if snapshot information is loaded
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    //Simulate log recovery
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       1,  // sn
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);
    //Simulate recovery of the next operation
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    //Simulate recovery of the last operation
    offset = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check if the chunk information is correct
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    //Chunk data is overwritten
    char readbuf[2 * PAGE_SIZE];
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    //Raw data cow to snapshot
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 *Abnormal Test 12
 *Use case: PasteChunk, when data is written halfway and the metapage has not been updated, restart/crash
 *Expected: Reboot successful, pass successful
 */
TEST_F(ExceptionTestSuit, ExceptionTest12) {
    ChunkID id = 1;
    SequenceNum sn = 2;
    SequenceNum correctedSn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo info;
    std::string location("test@s3");

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

    //Construct data to be written and request offset
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    offset = 0;
    length = PAGE_SIZE;
    //Write data to chunk file through lfs
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    //Write data
    lfs_->Write(fd, buf1, offset + PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    //Simulate restart
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.blockSize = BLOCK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    //Construct a new dataStore_, And reinitialize, restart failed
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    //Simulate log recovery
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,  // corrected sn
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    errorCode = dataStore_->PasteChunk(1,  // id
                                       buf1,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    //Check Bitmap
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(1, info.bitmap->NextClearBit(0));

    //Read data verification
    char readbuf[2 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(1,  // id
                                      sn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

}  // namespace chunkserver
}  // namespace curve
