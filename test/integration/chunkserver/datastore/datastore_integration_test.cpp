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
const string baseDir = "./data_int";    // NOLINT
const string poolDir = "./chunkfilepool_int";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int.meta";  // NOLINT

class DatastoreIntegrationTest : public DatastoreIntegrationBase {
 public:
    DatastoreIntegrationTest() {}
    ~DatastoreIntegrationTest() {}
};

/**
 * 基本功能测试验证
 * 读、写、删、获取文件信息
 */
TEST_F(DatastoreIntegrationTest, BasicTest) {
    ChunkID id = 1;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(id);
    CSErrorCode errorCode;
    CSChunkInfo info;

    /******************场景一：新建的文件，Chunk文件不存在******************/

    // 文件不存在
    ASSERT_FALSE(lfs_->FileExists(chunkPath));

    // 读chunk时返回ChunkNotExistError
    char readbuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // 无法获取到chunk的版本号
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // 删除chunk返回Success
    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    /******************场景二：通过WriteChunk产生chunk文件后操作**************/

    char buf1_1_1[PAGE_SIZE];
    memset(buf1_1_1, 'a', length);
    // 第一次WriteChunk会产生chunk文件
    errorCode = dataStore_->WriteChunk(id,
                                        sn,
                                        buf1_1_1,
                                        offset,
                                        length,
                                        nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 可以获取到chunk的信息，且各项信息符合预期
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

    // 读取写入的4KB验证一下,应当与写入数据相等
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_1, readbuf, length));

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    errorCode = dataStore_->ReadChunk(id,
                                      sn,
                                      readbuf,
                                      CHUNK_SIZE - PAGE_SIZE,
                                      length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // chunk 存在时，覆盖写
    char buf1_1_2[PAGE_SIZE];
    memset(buf1_1_2, 'b', length);
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       buf1_1_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    errorCode = dataStore_->ReadChunk(id,
                                      sn,
                                      readbuf,
                                      offset,
                                      3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, length));

    // chunk 存在时，写入未写过区域
    char buf1_1_3[PAGE_SIZE];
    memset(buf1_1_3, 'c', length);
    offset = PAGE_SIZE;
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       buf1_1_3,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    errorCode = dataStore_->ReadChunk(id,
                                      sn,
                                      readbuf,
                                      0,
                                      3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_3, readbuf + PAGE_SIZE, PAGE_SIZE));

    // chunk 存在时，覆盖部分区域
    char buf1_1_4[2 * PAGE_SIZE];
    memset(buf1_1_4, 'd', length);
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       buf1_1_4,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    errorCode = dataStore_->ReadChunk(id,
                                      sn,
                                      readbuf,
                                      0,
                                      3 * PAGE_SIZE);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_2, readbuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_1_4, readbuf + PAGE_SIZE, 2 * PAGE_SIZE));


    /******************场景三：用户删除文件******************/

    errorCode = dataStore_->DeleteChunk(id, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
    ASSERT_FALSE(lfs_->FileExists(chunkPath));
}

/**
 * 重启恢复测试
 */
TEST_F(DatastoreIntegrationTest, RestartTest) {
    SequenceNum fileSn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    CSChunkInfo info1;
    CSChunkInfo info2;
    CSChunkInfo info3;
    std::string location("test@s3");

    // 构造要用到的读写缓冲区
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

    // 各个操作对应的错误码返回值，错误码命名格式为 e_optype_chunid_sn
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

    // 模拟所有用户请求，用lamdba函数可以用于验证日志恢复时重用这部分代码
    // 如果后面要加用例，只需要在函数内加操作即可
    auto ApplyRequests = [&]() {
        fileSn = 1;
        // 模拟普通文件操作，WriteChunk产生chunk1、chunk2
        offset = 0;
        length = 2 * PAGE_SIZE;
        // 产生chunk1
        e_write_1_1 = dataStore_->WriteChunk(1,    // chunk id
                                        fileSn,
                                        buf1_1,
                                        offset,
                                        length,
                                        nullptr);
        // 产生chunk2
        e_write_2_1 = dataStore_->WriteChunk(2,    // chunk id
                                        fileSn,
                                        buf1_1,
                                        offset,
                                        length,
                                        nullptr);
        // 删除chunk1
        e_del_1_1 = dataStore_->DeleteChunk(1, fileSn);

        // 模拟快照操作
        ++fileSn;
        offset = 1 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        // 写chunk2，产生快照文件
        e_write_2_2 = dataStore_->WriteChunk(2,    // chunk id
                                        fileSn,
                                        buf2_2,
                                        offset,
                                        length,
                                        nullptr);
        // 删除chunk2快照
        e_delsnap_2_2 = dataStore_->DeleteSnapshotChunk(2, fileSn);
        // 模拟再次快照，然后删除chunk2快照
        ++fileSn;
        e_delsnap_2_3 = dataStore_->DeleteSnapshotChunk(2, fileSn);
        // 模拟再次快照，然后写数据到chunk2产生快照
        ++fileSn;
        offset = 2 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        // 写chunk2，产生快照文件
        e_write_2_4 = dataStore_->WriteChunk(2,    // chunk id
                                        fileSn,
                                        buf2_4,
                                        offset,
                                        length,
                                        nullptr);

        // 模拟克隆操作
        e_clone_3_1 = dataStore_->CreateCloneChunk(3,  // chunk id
                                                1,  // sn
                                                0,  // corrected sn
                                                CHUNK_SIZE,
                                                location);
        // 写数据到chunk3
        offset = 0;
        length = 2 * PAGE_SIZE;
        // 写chunk3
        e_write_3_1 = dataStore_->WriteChunk(3,    // chunk id
                                        1,    // sn
                                        writeBuf,
                                        offset,
                                        length,
                                        nullptr);
        // paste数据到chunk3
        offset = 1 * PAGE_SIZE;
        length = 2 * PAGE_SIZE;
        e_paste_3_1 = dataStore_->PasteChunk(3,    // chunk id
                                        pasteBuf,
                                        offset,
                                        length);
    };

    // 检查上面用户操作以后，DataStore层各文件的状态，可重用
    auto CheckStatus = [&]() {
        CSErrorCode errorCode;
        // chunk1 不存在
        errorCode = dataStore_->GetChunkInfo(1, &info1);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
        // chunk2存在，版本为4，correctedSn为3，存在快照，快照版本为2
        errorCode = dataStore_->GetChunkInfo(2, &info2);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(4, info2.curSn);
        ASSERT_EQ(2, info2.snapSn);
        ASSERT_EQ(3, info2.correctedSn);
        // 检查chunk2数据，[0, 1KB]:a , [1KB, 2KB]:b , [2KB, 4KB]:c
        errorCode = dataStore_->ReadChunk(2, fileSn, readBuf, 0, readSize);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf2_1, readBuf, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_2, readBuf + 1 * PAGE_SIZE, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_4, readBuf + 2 * PAGE_SIZE, 2 * PAGE_SIZE));
        // 检查chunk2快照数据，[0, 1KB]:a , [1KB, 3KB]:b
        errorCode = dataStore_->ReadSnapshotChunk(2, 2, readBuf, 0, readSize);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf2_1, readBuf, 1 * PAGE_SIZE));
        ASSERT_EQ(0, memcmp(buf2_2, readBuf + 1 * PAGE_SIZE, 2 * PAGE_SIZE));
    };

    /******************构造重启前的数据******************/
    // 提交操作
    ApplyRequests();
    // 检查每次操作的返回值是否符合预期
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
    // 检查此时各个文件的状态
    CheckStatus();

    /******************场景一：重启重新加载文件******************/
    // 模拟重启
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.pageSize = PAGE_SIZE;
    // 构造新的dataStore_，并重新初始化
    dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                               filePool_,
                                               options);
    ASSERT_TRUE(dataStore_->Initialize());
    // 检查各个chunk的状态，应该与前面的一致
    CheckStatus();

    /******************场景二：恢复日志，重放之前的操作******************/
    // 模拟日志回放
    ApplyRequests();
    // 检查每次操作的返回值是否符合预期
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
