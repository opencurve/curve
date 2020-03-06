/*
 * Project: curve
 * Created Date: 2020-03-10
 * Author: qinyi
 * Copyright (c) 2020 netease
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
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int WriteChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                          SequenceNum sn, off_t offset, size_t len,
                          const char *data);

    /**
     * @brief 通过chunkService读chunk
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param data 读取内容
     * @return 请求执行失败则返回-1，否则返回错误码
     */
    static int ReadChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                         SequenceNum sn, off_t offset, size_t len,
                         string *data);

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
     * @brief  通过chunkService获取chunk元数据
     * @param opConf，leaderPeer/copysetid等公共配置参数
     * @param chunkId
     * @param curSn 返回的当前chunk版本，未获取到则返回-1
     * @param snapSn 返回的快照chunk版本，未获取到则返回-1
     * @param redirectedLeader 返回的重定向主节点
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
     * @return 返回写操作的错误码
     */
    int VerifyWriteChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                         size_t len, const char *data, string *chunkData);

    /**
     * @brief 执行读chunk, 并验证读取内容是否与chunkdata对应区域的预期数据吻合。
     * @param chunkId
     * @param sn chunk版本
     * @param offset
     * @param len
     * @param chunkData 整个chunk的预期数据
     * @return 读请求结果符合预期返回0，否则返回-1
     */
    int VerifyReadChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                        size_t len, string *chunkData);

    /**
     * @brief 通过chunkService删除chunk，并更新
     * @param chunkId
     * @param sn chunk版本
     * @return 返回删除操作的错误码
     */
    int VerifyDeleteChunk(ChunkID chunkId, SequenceNum sn);

    /**
     * @brief 获取chunk元数据，并检验结果是否符合预期
     * @param chunkId
     * @param expCurSn 预期chunk版本
     * @param expSanpSn 预期快照版本
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
