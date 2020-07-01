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
 * 克隆场景测试
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

    /******************场景一：创建克隆文件******************/

    // 创建克隆文件chunk1
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,
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
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    // 再次调该接口，仍返回成功，chunk的信息不变
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             correctedSn,
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
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    // 创建克隆文件chunk2
    errorCode = dataStore_->CreateCloneChunk(2,  // chunk id
                                             sn,
                                             correctedSn,
                                             CHUNK_SIZE,
                                             location);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(2, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(2, info.chunkId);
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    /******************场景二：恢复克隆文件******************/
    // 构造原始数据
    char pasteBuf[4 * PAGE_SIZE];
    memset(pasteBuf, '1', 4 * PAGE_SIZE);
    // WriteChunk写数据到clone chunk的[0, 8KB]区域
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
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    // 读Chunk数据，[0, 8KB]数据应为‘1’
    size_t readSize = 2 * PAGE_SIZE;
    char readBuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, readSize));

    // PasteChunk再次写数据到clone chunk的[0, 8KB]区域
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    // 读Chunk数据，[0, 8KB]数据应为‘a’
    readSize = 2 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, readSize));

    // WriteChunk再次写数据到clone chunk的[4KB, 12KB]区域
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
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(3, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    // 读Chunk数据，[0, 4KB]数据应为‘a’，[4KB, 12KB]数据应为‘c’
    readSize = 3 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf1, readBuf, PAGE_SIZE));
    ASSERT_EQ(0, memcmp(writeBuf3, readBuf + PAGE_SIZE, 2 * PAGE_SIZE));

    /******************场景三：clone文件遍写后转换为普通chunk文件*************/

    char overBuf[1 * kMB] = {0};
    for (int i = 0; i < 16; ++i) {
        errorCode = dataStore_->PasteChunk(id,
                                           overBuf,
                                           i * kMB,   // offset
                                           1 * kMB);  // length
        ASSERT_EQ(errorCode, CSErrorCode::Success);
    }
    // 检查chunk的各项信息，都符合预期,chunk转为了普通的chunk
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(false, info.isClone);
    ASSERT_EQ(nullptr, info.bitmap);

    /******************场景三：删除文件****************/

    // 此时删除Chunk1，返回Success
    errorCode = dataStore_->DeleteChunk(1, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(1, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);

    // 此时删除Chunk2，返回Success
    errorCode = dataStore_->DeleteChunk(2, sn);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    errorCode = dataStore_->GetChunkInfo(2, &info);
    ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
}

/**
 * 恢复场景测试
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

    /******************场景一：创建克隆文件******************/

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
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    // 再次调该接口，仍返回成功，chunk的信息不变
    errorCode = dataStore_->CreateCloneChunk(id,  // chunk id
                                             sn,
                                             3,  // corrected sn
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
    ASSERT_EQ(PAGE_SIZE, info.pageSize);
    ASSERT_EQ(CHUNK_SIZE, info.chunkSize);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));

    /******************场景二：恢复克隆文件******************/
    sn = 3;
    // 构造原始数据
    char pasteBuf[4 * PAGE_SIZE];
    memset(pasteBuf, '1', 4 * PAGE_SIZE);
    // PasteChunk写数据到clone chunk的[0, 8KB]区域
    offset = 0;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    // 读Chunk数据，[0, 8KB]数据应为‘1’
    size_t readSize = 2 * PAGE_SIZE;
    char readBuf[3 * PAGE_SIZE];
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(pasteBuf, readBuf, readSize));

    // WriteChunk再次写数据到clone chunk的[0, 8KB]区域
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
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(2, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(2));
    // 读Chunk数据，[0, 8KB]数据应为‘b’
    readSize = 2 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf2, readBuf, readSize));

    // PasteChunk再次写数据到clone chunk的[4KB, 12KB]区域
    offset = PAGE_SIZE;
    length = 2 * PAGE_SIZE;
    errorCode = dataStore_->PasteChunk(id,
                                       pasteBuf,
                                       offset,
                                       length);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    // 检查chunk的各项信息，都符合预期
    errorCode = dataStore_->GetChunkInfo(id, &info);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(sn, info.curSn);
    ASSERT_EQ(0, info.snapSn);
    ASSERT_EQ(correctedSn, info.correctedSn);
    ASSERT_EQ(true, info.isClone);
    ASSERT_NE(nullptr, info.bitmap);
    // 写入PAGE对应bit置为1，其余都为0
    ASSERT_EQ(0, info.bitmap->NextSetBit(0));
    ASSERT_EQ(3, info.bitmap->NextClearBit(0));
    ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    // 读Chunk数据，[0, 8KB]数据应为‘b’，[8KB, 12KB]数据应为‘1’
    readSize = 3 * PAGE_SIZE;
    memset(readBuf, 0, sizeof(readBuf));
    errorCode = dataStore_->ReadChunk(id, sn, readBuf, 0, readSize);
    ASSERT_EQ(errorCode, CSErrorCode::Success);
    ASSERT_EQ(0, memcmp(writeBuf2, readBuf, 2 * PAGE_SIZE));
    ASSERT_EQ(0, memcmp(pasteBuf, readBuf + 2 * PAGE_SIZE, PAGE_SIZE));

    /******************场景三：clone文件遍写后转换为普通chunk文件*************/

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
    // 检查chunk的各项信息，都符合预期,chunk转为了普通的chunk
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
