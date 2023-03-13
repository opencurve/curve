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
 * 异常测试1
 * 用例：chunk的metapage数据损坏，然后启动DataStore
 * 预期：重启失败
 */
TEST_F(ExceptionTestSuit, ExceptionTest1) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 通过lfs修改chunk1的metapage
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    // 修改metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 * 异常测试2
 * 用例：chunk的metapage数据损坏，然后更新了metapage，然后重启DataStore
 * 预期：重启datastore可以成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest2) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 通过lfs修改chunk1的metapage
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    // 修改metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 触发metapage更新
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());
}

/**
 * 异常测试3
 * 用例：chunk快照的metapage数据损坏，然后重启DataStore
 * 预期：重启失败
 */
TEST_F(ExceptionTestSuit, ExceptionTest3) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 生成快照文件
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 通过lfs修改chunk1快照的metapage
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    // 修改metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 * 异常测试4
 * 用例：chunk快照的metapage数据损坏，但是更新了metapage，然后重启DataStore
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest4) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf[PAGE_SIZE];
    memset(buf, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 生成快照文件
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 触发快照metapage更新
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf,
                                       offset + PAGE_SIZE,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 通过lfs修改chunk1快照的metapage
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    char metapage[PAGE_SIZE];
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metapage, 0, PAGE_SIZE);
    // 修改metapage
    metapage[0]++;
    lfs_->Write(fd, metapage, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_FALSE(dataStore_->Initialize());
}

/**
 * 异常测试5
 * 用例：WriteChunk数据写到一半重启
 * 预期：重启成功，重新执行上一条操作成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest5) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 构造要写入的数据和请求偏移
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', length);
    offset = 0;
    length = 2 * PAGE_SIZE;
    // 通过lfs写一半数据到chunk文件
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    // 写数据
    lfs_->Write(fd, buf2, offset + PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 模拟日志恢复
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 读数据校验
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
 * 异常测试6
 * 用例：WriteChunk更新metapage后重启，sn>chunk.sn,sn==chunk.correctedSn
 * 预期：重启成功，重新执行上一条操作成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest6) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 更新 correctedsn 为2
    errorCode = dataStore_->DeleteSnapshotChunk(1, 2);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 构造要写入的请求参数
    char buf2[2 * PAGE_SIZE];
    memset(buf2, '2', length);
    offset = 0;
    length = 2 * PAGE_SIZE;
    fileSn = 2;  // sn > chunk.sn; sn == chunk.correctedSn

    // 通过lfs修改chunk1的metapage
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metabuf[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);

    // 模拟更新metapage成功
    ChunkFileMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, metaPage.sn);
    metaPage.sn = fileSn;
    metaPage.encode(metabuf);
    // 更新metapage
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 模拟日志恢复
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 读数据校验
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
 * 异常测试7
 * 用例：WriteChunk产生快照后重启，恢复历史操作和当前操作
 *      sn>chunk.sn, sn>chunk.correctedSn
 *      测chunk.sn>chunk.correctedSn
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest7) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1，模拟chunk.sn>chunk.correctedSn的情况
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟创建快照文件
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

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 检查是否加载了快照信息
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    // 模拟日志恢复前一条操作
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 读快照文件来校验是否有cow
    char readbuf[2 * PAGE_SIZE];
    snapshot.Read(readbuf, offset, length);
    // 预期未发生cow
    ASSERT_NE(0, memcmp(buf1, readbuf, length));

    // 模拟恢复最后一条操作
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
    // 检查是否更新了版本号
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    // chunk数据被覆盖
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    // 原数据cow到快照
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 * 异常测试8
 * 用例：WriteChunk产生快照后重启，
 *      sn>chunk.sn, sn>chunk.correctedSn
 *      测chunk.sn==chunk.correctedSn
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest8) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1,构造chunk.sn==chunk.correctedsn的场景
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->DeleteSnapshotChunk(1, 2);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       ++fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟创建快照文件
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

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 检查是否加载了快照信息
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(2, info.snapSn);
    ASSERT_EQ(2, info.correctedSn);

    // 模拟日志恢复前一条操作
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 读快照文件来校验是否有cow
    char readbuf[2 * PAGE_SIZE];
    snapshot.Read(readbuf, offset, length);
    // 预期未发生cow
    ASSERT_NE(0, memcmp(buf1, readbuf, length));

    // 模拟恢复最后一条操作
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
    // 检查是否更新了版本号
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);
    ASSERT_EQ(2, info.correctedSn);
    // chunk数据被覆盖
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    // 原数据cow到快照
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              2,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 * 异常测试9
 * 用例：WriteChunk产生快照并更新metapage后重启，恢复历史操作和当前操作
 *      sn>chunk.sn, sn>chunk.correctedSn
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest9) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1，模拟chunk.sn>chunk.correctedSn的情况
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟创建快照文件
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

    // 通过lfs修改chunk1的metapage
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    char metabuf[PAGE_SIZE];
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);

    // 模拟更新metapage成功
    ChunkFileMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(1, metaPage.sn);
    metaPage.sn = 2;
    metaPage.encode(metabuf);
    // 更新metapage
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 检查是否加载了快照信息
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    // 模拟日志恢复前一条操作
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);

    // 模拟恢复最后一条操作
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
    // 检查是否更新了版本号
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    // chunk数据被覆盖
    char readbuf[2 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(1,  // id
                                      fileSn,
                                      readbuf,
                                      offset,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2, readbuf, length));
    // 原数据cow到快照
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 * 异常测试10
 * 用例：WriteChunk更新快照metapage前重启，恢复历史操作和当前操作
 *      sn>chunk.sn, sn>chunk.correctedSn
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest10) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = 2 * PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1，模拟chunk.sn>chunk.correctedSn的情况
    char buf1[2 * PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 产生快照文件
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

    // 模拟cow
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    // 写数据
    lfs_->Write(fd, buf1, 2 * PAGE_SIZE, PAGE_SIZE);
    // 更新metapage
    char metabuf[PAGE_SIZE];
    lfs_->Read(fd, metabuf, 0, PAGE_SIZE);
    // 修改metapage
    SnapshotMetaPage metaPage;
    errorCode = metaPage.decode(metabuf);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    metaPage.bitmap->Set(1);
    metaPage.encode(metabuf);
    lfs_->Write(fd, metabuf, 0, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 检查是否加载了快照信息
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    // 模拟日志恢复
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       1,  // sn
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);
    // 模拟恢复下一个操作
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟恢复最后一条操作
    offset = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk 信息是否正确
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    // chunk数据被覆盖
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
    // 原数据cow到快照
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 * 异常测试11
 * 用例：WriteChunk更新快照metapage后重启，恢复历史操作和当前操作
 *      sn>chunk.sn, sn>chunk.correctedSn
 * 预期：重启成功
 */
TEST_F(ExceptionTestSuit, ExceptionTest11) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = 2 * PAGE_SIZE;
    CSErrorCode errorCode;
    CSChunkInfo chunk1Info;

    // 生成chunk1，模拟chunk.sn>chunk.correctedSn的情况
    char buf1[2 * PAGE_SIZE];
    memset(buf1, '1', length);
    errorCode = dataStore_->WriteChunk(1,  // id
                                       fileSn,
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 产生快照文件
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

    // 模拟cow
    std::string snapPath = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(1, 1);
    int fd = lfs_->Open(snapPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    // 写数据
    lfs_->Write(fd, buf1, 2 * PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    options.blockSize = BLOCK_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 检查是否加载了快照信息
    CSChunkInfo info;
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);

    // 模拟日志恢复
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       1,  // sn
                                       buf1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::BackwardRequestError);
    // 模拟恢复下一个操作
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 模拟恢复最后一条操作
    offset = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(1,  // id
                                       2,  // sn
                                       buf2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk 信息是否正确
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);
    ASSERT_EQ(0, info.correctedSn);
    // chunk数据被覆盖
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
    // 原数据cow到快照
    errorCode = dataStore_->ReadSnapshotChunk(1,  // id
                                              1,
                                              readbuf,
                                              offset,
                                              length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1, readbuf, length));
}

/**
 * 异常测试12
 * 用例：PasteChunk，数据写入一半时，还未更新metapage重启/崩溃
 * 预期：重启成功,paste成功
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

    // 创建克隆文件chunk1
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,  // corrected sn
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk的各项信息，都符合预期
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

    // 构造要写入的数据和请求偏移
    char buf1[PAGE_SIZE];
    memset(buf1, '1', length);
    offset = 0;
    length = PAGE_SIZE;
    // 通过lfs写数据到chunk文件
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(1);
    int fd = lfs_->Open(chunkPath, O_RDWR|O_NOATIME|O_DSYNC);
    ASSERT_GT(fd, 0);
    // 写数据
    lfs_->Write(fd, buf1, offset + PAGE_SIZE, PAGE_SIZE);
    lfs_->Close(fd);

    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.blockSize = BLOCK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    // 构造新的dataStore_，并重新初始化,重启失败
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());

    // 模拟日志恢复
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
    // 检查bitmap
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(1, info.bitmap->NextClearBit(0));

    // 读数据校验
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
