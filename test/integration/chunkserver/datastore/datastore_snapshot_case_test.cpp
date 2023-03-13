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

const string baseDir = "./data_int_sna";    // NOLINT
const string poolDir = "./chunkfilepool_int_sna";  // NOLINT
const string poolMetaPath = "./chunkfilepool_int_sna.meta";  // NOLINT

class SnapshotTestSuit : public DatastoreIntegrationBase {
 public:
    SnapshotTestSuit() {}
    ~SnapshotTestSuit() {}
};

/**
 * 快照场景测试
 * 构造存在两个chunk的文件，分别为chunk1和chunk2，做如下操作
 * 1.写chunk1
 * 2.模拟第一次打快照，转储过程中写chunk1并产生快照，chunk2未发生数据写入
 * 3.删除快照，然后向chunk2中写入数据
 * 4.模拟第二次打快照，转储过程中写chunk1，但是不写chunk2
 * 5.删除快照，再次向chunk2写入数据
 * 6.删除文件
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

    /******************构造初始环境，创建chunk1******************/

    // 向chunk1的[0, 12KB)区域写入数据 "1"
    offset = 0;
    length = 3 * PAGE_SIZE;  // 12KB
    char buf1_1[3 * PAGE_SIZE];
    memset(buf1_1, '1', length);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_1,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    /******************场景一：第一次给文件打快照******************/

    // 模拟打快照，此时文件版本递增
    ++fileSn;   // fileSn == 2

    // 向chunk1的[4KB, 8KB)区域写入数据 “2”
    offset = 1 * PAGE_SIZE;
    length = 1 * PAGE_SIZE;
    char buf1_2[3 * PAGE_SIZE];
    memset(buf1_2, '2', 3 * PAGE_SIZE);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 可以获取到chunk1的信息，且各项信息符合预期
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(1, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    size_t readSize = 3 * PAGE_SIZE;
    char readbuf[3 * PAGE_SIZE];
    // 读chunk1快照文件的[0, 12KB)区域，读出来数据应该都是‘1’
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,  // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1, readbuf, readSize));

    // 重复写入，验证不会重复cow，读快照时[4KB, 8KB)区域的数据应为“1”
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 写未cow过的区域，写入[0,4kb]区域
    offset = 0;
    length = PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 写部分cow过的区域，写入[4kb,12kb]区域
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 可以获取到chunk1的信息，且各项信息符合预期
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, chunk1Info.curSn);
    ASSERT_EQ(1, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // 此时读chunk1返回数据内容应该为[0,12KB]:2
    // 读chunk1快照返回的数据内容应该为[0, 12KB):1
    // 其余地址空间的数据可以不用保证
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id1,    // chunk id
                                      fileSn,
                                      readbuf,
                                      0,    // offset
                                      readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_2, readbuf, readSize));

    // 读chunk1快照文件的[0, 12KB)区域，读出来数据应该还是‘1’
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,  // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_1, readbuf, readSize));

    // ReadSnapshotChun，请求offset+length > page size
    offset = CHUNK_SIZE - PAGE_SIZE;
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              1,  // snap sn
                                              readbuf,
                                              offset,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::InvalidArgError);

    // 读chunk2快照文件,返回ChunkNotExistError
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id2,  // chunk id
                                              1,  // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    /******************场景二：第一次快照结束，删除快照******************/

    // 请求删chunk1的快照，返回成功，并删除快照
    errorCode = dataStore_->DeleteSnapshotChunk(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk1信息，符合预期
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(0, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // 请求删chunk2的快照，返回成功
    errorCode = dataStore_->DeleteSnapshotChunk(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);

    // 向chunk2的[0, 8KB)区域写入数据 "a"
    offset = 0;
    length = 2 * PAGE_SIZE;  // 8KB
    char buf2_2[2 * PAGE_SIZE];
    memset(buf2_2, 'a', length);
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn,
                                       buf2_2,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk1信息，符合预期
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(0, chunk2Info.correctedSn);

    /******************场景三：第二次打快照******************/

    // 模拟第二次打快照，版本递增
    ++fileSn;  // fileSn == 3

    // 向chunk1的[0KB, 8KB)区域写入数据 “3”
    offset = 0;
    length = 2 * PAGE_SIZE;
    char buf1_3[2 * PAGE_SIZE];
    memset(buf1_3, '3', length);
    errorCode = dataStore_->WriteChunk(id1,  // id
                                       fileSn,
                                       buf1_3,
                                       offset,
                                       length,
                                       nullptr);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 可以获取到chunk1的信息，且各项信息符合预期
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(2, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // 此时读chunk1返回数据内容应该为[0,8KB]:3,[8KB, 12KB]:2
    // 读chunk1快照返回的数据内容应该为[0, 12KB]:2
    // 其余地址空间的数据可以不用保证
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadChunk(id1,    // chunk id
                                      fileSn,
                                      readbuf,
                                      0,    // offset
                                      readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_3, readbuf, 2 * PAGE_SIZE));
    ASSERT_EQ(0, memcmp(buf1_2, readbuf + 2 * PAGE_SIZE, 1 * PAGE_SIZE));

    // 读chunk1快照文件的[0, 12KB)区域,数据内容为‘2’
    readSize = 3 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id1,  // chunk id
                                              2,  // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf1_2, readbuf, readSize));

    // 读chunk2快照返回的数据内容应该为[0, 8KB):a,其余地址空间的数据可以不用保证
    readSize = 2 * PAGE_SIZE;
    memset(readbuf, 0, sizeof(readbuf));
    errorCode = dataStore_->ReadSnapshotChunk(id2,  // chunk id
                                              2,  // snap sn
                                              readbuf,
                                              0,  // offset
                                              readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(buf2_2, readbuf, readSize));

    /******************场景四：第二次快照结束，删除快照******************/

    // 请求删chunk1的快照，返回成功
    errorCode = dataStore_->DeleteSnapshotChunk(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk1信息，符合预期
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk1Info.curSn);
    ASSERT_EQ(0, chunk1Info.snapSn);
    ASSERT_EQ(0, chunk1Info.correctedSn);

    // 请求删chunk2的快照，返回成功
    errorCode = dataStore_->DeleteSnapshotChunk(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk2信息，符合预期
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    // 向chunk2的[0KB, 4KB)区域写入数据 “b”
    offset = 0;
    length = 1 * PAGE_SIZE;
    char buf2_3[1 * PAGE_SIZE];
    memset(buf2_3, 'b', length);
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn,
                                       buf2_3,
                                       offset,
                                       length,
                                       nullptr);
    // 检查chunk2信息，符合预期,curSn变为3，不会产生快照
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    // 再次向chunk2的[0KB, 8KB)区域写入数据
    errorCode = dataStore_->WriteChunk(id2,  // id
                                       fileSn,
                                       buf2_3,
                                       offset,
                                       length,
                                       nullptr);
    // 检查chunk2信息，chunk信息不变，不会产生快照
    errorCode = dataStore_->GetChunkInfo(id2, &chunk2Info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(fileSn, chunk2Info.curSn);
    ASSERT_EQ(0, chunk2Info.snapSn);
    ASSERT_EQ(fileSn, chunk2Info.correctedSn);

    /******************场景五：用户删除文件******************/

    // 此时删除Chunk1，返回Success
    errorCode = dataStore_->DeleteChunk(id1, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id1, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // 此时删除Chunk2，返回Success
    errorCode = dataStore_->DeleteChunk(id2, fileSn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(id2, &chunk1Info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
}

}  // namespace chunkserver
}  // namespace curve
