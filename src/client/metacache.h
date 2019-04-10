/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:22 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef SRC_CLIENT_METACACHE_H_
#define SRC_CLIENT_METACACHE_H_

#include <string>
#include <list>
#include <map>
#include <vector>
#include <unordered_map>

#include "src/client/client_config.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"
#include "src/client/service_helper.h"

using curve::common::RWLock;

namespace curve {
namespace client {
enum MetaCacheErrorType {
    OK = 0,
    CHUNKINFO_NOT_FOUND = 1,
    LEADERINFO_NOT_FOUND = 2,
    SERVERLIST_NOT_FOUND = 3,
    UNKNOWN_ERROR
};
class MetaCache {
 public:
    using CopysetLogicPoolIDStr      = std::string;
    using ChunkInfoMap               = std::unordered_map<ChunkID, ChunkIDInfo_t>;       // NOLINT
    using CopysetInfoMap             = std::unordered_map<CopysetLogicPoolIDStr, CopysetInfo_t>;            // NOLINT
    using ChunkIndexInfoMap          = std::map<ChunkIndex, ChunkIDInfo_t>;

    MetaCache();
    virtual ~MetaCache();

    /**
     * 初始化函数
     * @param: metacacheopt为当前metacache的配置option信息
     */
    void Init(MetaCacheOption_t metaCacheOpt);

    /**
     * 通过chunk index获取chunkid信息
     * @param: chunkidx以index查询chunk对应的id信息
     * @param: chunkinfo是出参，存储chunk的版本信息
     * @param: 成功返回OK, 否则返回UNKNOWN_ERROR
     */
    virtual MetaCacheErrorType GetChunkInfoByIndex(ChunkIndex chunkidx,
                                ChunkIDInfo_t* chunkinfo);
    /**
     * 通过chunkid获取chunkinfo id信息
     * @param: chunkid是待查询的chunk id信息
     * @param: chunkinfo是出参，存储chunk的版本信息
     * @param: 成功返回OK, 否则返回UNKNOWN_ERROR
     */
    virtual MetaCacheErrorType GetChunkInfoByID(ChunkID chunkid,
                                ChunkIDInfo_t* chunkinfo);

    /**
     * sender发送数据的时候需要知道对应的leader然后发送给对应的chunkserver
     * 如果get不到的时候，外围设置refresh为true，然后向chunkserver端拉取最新的
     * server信息，然后更新metacache
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: serverId对应chunkserver的id信息，是出参
     * @param: serverAddr为serverid对应的ip信息
     * @param: refresh，如果get不到的时候，外围设置refresh为true，
     *         然后向chunkserver端拉取最新的
     * @param: 成功返回0， 否则返回-1
     */
    virtual int GetLeader(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                ChunkServerID* serverId,
                                butil::EndPoint* serverAddr,
                                bool refresh = false);
    /**
     * 更新某个copyset的leader信息
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: serverId对应chunkserver的id信息
     * @param: leaderAddr为serverid对应的ip信息
     * @return: 成功返回0， 否则返回-1
     */
    virtual int UpdateLeader(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                ChunkServerID* serverid,
                                const butil::EndPoint &leaderAddr);
    /**
     * 更新copyset数据信息，包含serverlist
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: csinfo是要更新的copyset info
     */
    virtual void UpdateCopysetInfo(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                CopysetInfo_t csinfo);
    /**
     * 通过chunk index更新chunkid信息
     * @param: index为待更新的chunk index
     * @param: chunkinfo为需要更新的info信息
     */
    virtual void UpdateChunkInfoByIndex(ChunkIndex cindex,
                                ChunkIDInfo_t chunkinfo);
    /**
     * 通过chunk id更新chunkid信息
     * @param: cid为chunkid
     * @param: cidinfo为当前chunk对应的id信息
     */
    virtual void UpdateChunkInfoByID(ChunkID cid, ChunkIDInfo cidinfo);

    /**
     * 当读写请求返回后，更新当前copyset的applyindex信息
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: appliedindex是需要更新的applyindex
     */
    virtual void UpdateAppliedIndex(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                uint64_t appliedindex);
    /**
     * 当读数据时，需要获取当前copyset的applyindex信息
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @return: 当前copyset的applyin信息
     */
    uint64_t GetAppliedIndex(LogicPoolID logicPoolId,
                                CopysetID copysetId);

    /**
     * 获取当前copyset的server list信息
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @return: 当前copyset的copysetinfo信息
     */
    virtual CopysetInfo_t GetServerList(LogicPoolID logicPoolId,
                                        CopysetID copysetId);

    /**
     * 将ID转化为cache的key
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @return: 为当前的key
     */
    inline std::string LogicPoolCopysetID2Str(LogicPoolID lpid,
                                        CopysetID csid);
    /**
     * 将ID转化为cache的key
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: chunkid是chunk的id
     * @return: 为当前的key
     */
    inline std::string LogicPoolCopysetChunkID2Str(LogicPoolID lpid,
                                        CopysetID csid,
                                        ChunkID chunkid);

 private:
    MetaCacheOption_t   metacacheopt_;

    // chunkindex到chunkidinfo的映射表
    CURVE_CACHELINE_ALIGNMENT ChunkIndexInfoMap     chunkindex2idMap_;

    // logicalpoolid和copysetid到copysetinfo的映射表
    CURVE_CACHELINE_ALIGNMENT CopysetInfoMap        lpcsid2serverlistMap_;

    // chunkid到chunkidinfo的映射表
    CURVE_CACHELINE_ALIGNMENT ChunkInfoMap          chunkid2chunkInfoMap_;

    // 三个读写锁分别保护上述三个映射表
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4chunkInfoMap_;
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4ChunkInfo_;
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4CopysetInfo_;
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_METACACHE_H_
