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
 * Created Date: 2020-03-10
 * Author: qinyi
 */

#ifndef TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_
#define TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_

#include <brpc/channel.h>
#include <string>
#include <set>
#include <memory>
#include "include/chunkserver/chunkserver_common.h"
#include "proto/common.pb.h"

namespace curve {
namespace chunkserver {

using curve::common::Peer;
using std::shared_ptr;
using std::string;

#define NULL_SN -1

struct ChunkServiceOpConf {
    Peer *leaderPeer;
    LogicPoolID logicPoolId;
    CopysetID copysetId;
    uint32_t rpcTimeout;
};

class ChunkServiceOp {
 public:
    /**
     * @brief 通过chunkService写chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param data 待写数据
     * @param cloneFileSource clone源的文件路径
     * @param cloneFileOffset clone chunk在clone源中的相对偏移
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int WriteChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                          SequenceNum sn, off_t offset, size_t len,
                          const char *data,
                          const std::string& cloneFileSource = "",
                          off_t cloneFileOffset = 0);

    /**
     * @brief 通过chunkService读chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param data 读取内容
     * @param cloneFileSource clone源的文件路径
     * @param cloneFileOffset clone chunk在clone源中的相对偏移
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int ReadChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                         SequenceNum sn, off_t offset, size_t len,
                         string *data,
                         const std::string& cloneFileSource = "",
                         off_t cloneFileOffset = 0);

    /**
     * @brief 通过chunkService读chunk快照
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param data 读取内容
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int ReadChunkSnapshot(struct ChunkServiceOpConf *opConf,
                                 ChunkID chunkId, SequenceNum sn, off_t offset,
                                 size_t len, std::string *data);

    /**
     * @brief 通过chunkService删除chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param sn chunk版本
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int DeleteChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                           SequenceNum sn);

    /**
     * @brief 通过chunkService删除此次转储时产生的或者历史遗留的快照
     *        如果转储过程中没有产生快照，则修改chunk的correctedSn
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param correctedSn
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int DeleteChunkSnapshotOrCorrectSn(struct ChunkServiceOpConf *opConf,
                                              ChunkID chunkId,
                                              SequenceNum correctedSn);

    /**
     * @brief 通过chunkService创建clone chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param location 源chunk在源端的位置,可能在curve或S3上
     * @param correctedSn
     * @param sn
     * @param chunkSize
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int CreateCloneChunk(struct ChunkServiceOpConf *opConf,
                                ChunkID chunkId, const std::string &location,
                                uint64_t correctedSn, uint64_t sn,
                                uint64_t chunkSize);

    /**
     * @brief 通过chunkService恢复chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param offset
     * @param len
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int RecoverChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                            off_t offset, size_t len);

    /**
     * @brief 通过chunkService获取chunk元数据
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param curSn 返回当前chunk版本
     * @param snapSn 返回快照chunk版本
     * @param redirectedLeader 返回重定向主节点
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int GetChunkInfo(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                            SequenceNum *curSn, SequenceNum *snapSn,
                            string *redirectedLeader);
};

class ChunkServiceVerify {
 public:
    explicit ChunkServiceVerify(struct ChunkServiceOpConf *opConf)
        : opConf_(opConf) {}

    /**
     * @brief 执行写chunk, 并将数据写入到chunkdata对应区域，以便于后续验证数据。
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param data 待写数据
     * @param chunkData 整个chunk的预期数据
     * @param cloneFileSource clone源的文件路径
     * @param cloneFileOffset clone chunk在clone源中的相对偏移
     * @return 返回写操作的错误码
     */
    int VerifyWriteChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                         size_t len, const char *data, string *chunkData,
                         const std::string& cloneFileSource = "",
                         off_t cloneFileOffset = 0);

    /**
     * @brief 执行读chunk, 并验证读取内容是否与chunkdata对应区域的预期数据吻合。
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param chunkData 整个chunk的预期数据
     * @param cloneFileSource clone源的文件路径
     * @param cloneFileOffset clone chunk在clone源中的相对偏移
     * @return 读请求结果符合预期返回0，否则返回-1
     */
    int VerifyReadChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                        size_t len, string *chunkData,
                        const std::string& cloneFileSource = "",
                        off_t cloneFileOffset = 0);

    /**
     * @brief 执行读chunk快照,
     * 并验证读取内容是否与chunkdata对应区域的预期数据吻合。
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param chunkData 整个chunk的预期数据
     * @return 读请求结果符合预期返回0，否则返回-1
     */
    int VerifyReadChunkSnapshot(ChunkID chunkId, SequenceNum sn, off_t offset,
                                size_t len, string *chunkData);

    /**
     * @brief 删除chunk
     * @param chunkId
     * @param sn chunk版本
     * @return 返回删除操作的错误码
     */
    int VerifyDeleteChunk(ChunkID chunkId, SequenceNum sn);

    /**
     * @brief 删除chunk的快照
     * @param chunkId
     * @param correctedSn
     * @return 返回删除操作的错误码
     */
    int VerifyDeleteChunkSnapshotOrCorrectSn(ChunkID chunkId,
                                             SequenceNum correctedSn);

    /**
     * @brief 创建clone chunk
     * @param chunkId
     * @param location 源地址
     * @param correctedSn
     * @param sn
     * @param chunkSize
     * @return 返回创建操作的错误码
     */
    int VerifyCreateCloneChunk(ChunkID chunkId, const std::string &location,
                               uint64_t correctedSn, uint64_t sn,
                               uint64_t chunkSize);

    /**
     * @brief 恢复chunk
     * @param chunkId
     * @param offset
     * @param len
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    int VerifyRecoverChunk(ChunkID chunkId, off_t offset, size_t len);

    /**
     * @brief 获取chunk元数据，并检验结果是否符合预期
     * @param chunkId
     * @param expCurSn 预期chunk版本，-1表示不存在
     * @param expSanpSn 预期快照版本，-1表示不存在
     * @param expLeader 预期redirectedLeader
     * @return 验证成功返回0，否则返回-1
     */
    int VerifyGetChunkInfo(ChunkID chunkId, SequenceNum expCurSn,
                           SequenceNum expSnapSn, string expLeader);

 private:
    struct ChunkServiceOpConf *opConf_;
    // 记录写过的chunkId（预期存在）,用于判断请求的返回值是否符合预期
    std::set<ChunkID> existChunks_;
};

}  // namespace chunkserver

}  // namespace curve

#endif  // TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_
