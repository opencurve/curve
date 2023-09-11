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

const string baseDir = "./data_int_bas";    // NOLINT
const string poolDir = "./chunkfilepool_int_bas";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_bas.meta";  // NOLINT

class BasicTestSuit : public DatastoreIntegrationBase {
 public:
    BasicTestSuit() {}
    ~BasicTestSuit() {}
};

//TODO: this unit test is suitable for the new version of datastore
//      and will be modified later dxiang@corp.netease.com
#if 0
/**
 * 基本功能测试验证
 * 读、写、删、获取文件信息
 */
TEST_F(BasicTestSuit, BasicTest) {
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
    ASSERT_EQ(PAGE_SIZE, info.metaPageSize);
    ASSERT_EQ(BLOCK_SIZE, info.blockSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);

    // 读取写入的4KB验证一下,应当与写入数据相等
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1_1, readbuf, length));

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    memset(readbuf, 0, sizeof(readbuf));
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
    memset(readbuf, 0, sizeof(readbuf));
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
    memset(readbuf, 0, sizeof(readbuf));
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

    butil::IOBuf iobuf1_1_4;
    iobuf1_1_4.append(buf1_1_4, length);

    errorCode = dataStore_->WriteChunk(id,
                                       sn,
                                       iobuf1_1_4,
                                       offset,
                                       length,
                                       nullptr,
                                       SnapContext::build_empty());
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 没被写过的区域也可以读，但是不保证读到的数据内容
    memset(readbuf, 0, sizeof(readbuf));
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
#endif

}  // namespace chunkserver
}  // namespace curve
