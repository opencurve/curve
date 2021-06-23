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
 * File Created: Tuesday, 25th September 2018 2:06:22 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_METACACHE_H_
#define SRC_CLIENT_METACACHE_H_

#include <set>
#include <string>
#include <unordered_map>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "src/client/service_helper.h"
#include "src/client/unstable_helper.h"
#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace client {

using curve::common::RWLock;

enum class MetaCacheErrorType {
    OK = 0,
    CHUNKINFO_NOT_FOUND = 1,
    LEADERINFO_NOT_FOUND = 2,
    SERVERLIST_NOT_FOUND = 3,
    UNKNOWN_ERROR
};

class MetaCache {
 public:
    using LogicPoolCopysetID = uint64_t;
    using ChunkInfoMap = std::unordered_map<ChunkID, ChunkIDInfo>;
    using CopysetInfoMap = std::unordered_map<LogicPoolCopysetID, CopysetInfo>;
    using ChunkIndexInfoMap = std::unordered_map<ChunkIndex, ChunkIDInfo>;

    MetaCache() = default;
    virtual ~MetaCache() = default;

    /**
     * 初始化函数
     * @param: metacacheopt为当前metacache的配置option信息
     * @param: mdsclient为与mds通信的指针。
     * 为什么这里需要把mdsclient传进来？
     * 因为首先metacache充当的角色就是对于MDS一侧的信息缓存
     * 所以对于底层想使用metacache的copyset client或者chunk closure
     * 来说，他只需要知道metacache就可以了，不需要再去向mds查询信息，
     * 在copyset client或者chunk closure发送IO失败之后会重新获取leader
     * 然后再重试，如果leader获取不成功，需要向mds一侧查询当前copyset的最新信息，
     * 这里将查询mds封装在内部了，这样copyset client和chunk closure就不感知mds了
     */
    void Init(const MetaCacheOption& metaCacheOpt, MDSClient* mdsclient);

    /**
     * 通过chunk index获取chunkid信息
     * @param: chunkidx以index查询chunk对应的id信息
     * @param: chunkinfo是出参，存储chunk的版本信息
     * @param: 成功返回OK, 否则返回UNKNOWN_ERROR
     */
    virtual MetaCacheErrorType GetChunkInfoByIndex(ChunkIndex chunkidx,
                                                   ChunkIDInfo_t* chunkinfo);

    /**
     * @brief Update cached chunk info by chunk index
     */
    virtual void UpdateChunkInfoByIndex(ChunkIndex cindex,
                                        const ChunkIDInfo& chunkinfo);

    /**
     * sender发送数据的时候需要知道对应的leader然后发送给对应的chunkserver
     * 如果get不到的时候，外围设置refresh为true，然后向chunkserver端拉取最新的
     * server信息，然后更新metacache。
     * 如果当前copyset的leaderMayChange置位的时候，即使refresh为false，也需要
     * 先去拉取新的leader信息，才能继续下发IO.
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: serverId对应chunkserver的id信息，是出参
     * @param: serverAddr为serverid对应的ip信息
     * @param: refresh，如果get不到的时候，外围设置refresh为true，
     *         然后向chunkserver端拉取最新的
     * @param: fm用于统计metric
     * @param: 成功返回0， 否则返回-1
     */
    virtual int GetLeader(LogicPoolID logicPoolId,
                          CopysetID copysetId,
                          ChunkServerID* serverId,
                          butil::EndPoint* serverAddr,
                          bool refresh = false,
                          FileMetric* fm = nullptr);
    /**
     * 更新某个copyset的leader信息
     * @param logicPoolId 逻辑池id
     * @param copysetId 复制组id
     * @param leaderAddr leader地址
     * @return: 成功返回0， 否则返回-1
     */
    virtual int UpdateLeader(LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             const butil::EndPoint& leaderAddr);
    /**
     * 更新copyset数据信息，包含serverlist
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @param: csinfo是要更新的copyset info
     */
    virtual void UpdateCopysetInfo(LogicPoolID logicPoolId,
                                   CopysetID copysetId,
                                   const CopysetInfo& csinfo);


    /**
     * 通过chunk id更新chunkid信息
     * @param: cid为chunkid
     * @param: cidinfo为当前chunk对应的id信息
     */
    virtual void UpdateChunkInfoByID(ChunkID cid, const ChunkIDInfo& cidinfo);

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
    uint64_t GetAppliedIndex(LogicPoolID logicPoolId, CopysetID copysetId);

    /**
     * 获取当前copyset的server list信息
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @return: 当前copyset的copysetinfo信息
     */
    virtual CopysetInfo GetServerList(LogicPoolID logicPoolId,
                                      CopysetID copysetId);

    /**
     * 将ID转化为cache的key
     * @param: lpid逻辑池id
     * @param: cpid是copysetid
     * @return: 为当前的key
     */
    static LogicPoolCopysetID CalcLogicPoolCopysetID(LogicPoolID logicPoolId,
                                                     CopysetID copysetId) {
        return (static_cast<uint64_t>(logicPoolId) << 32) |
               static_cast<uint64_t>(copysetId);
    }

    /**
     * @brief: 标记整个server上的所有chunkserver为unstable状态
     *
     * @param: serverIp server的ip地址
     * @return: 0 设置成功 / -1 设置失败
     */
    virtual int SetServerUnstable(const std::string& endPoint);

    /**
     * 如果leader所在的chunkserver出现问题了，导致RPC失败。这时候这个
     * chunkserver上的其他leader copyset也会存在同样的问题，所以需要
     * 通知当前chunkserver上的leader copyset. 主要是通过设置这个copyset
     * 的leaderMayChange标志，当该copyset的再次下发IO的时候会查看这个
     * 状态，当这个标志位置位的时候，IO下发需要先进行leader refresh，
     * 如果leaderrefresh成功，leaderMayChange会被reset。
     * SetChunkserverUnstable就会遍历当前chunkserver上的所有copyset
     * 并设置这个chunkserver的leader copyset的leaderMayChange标志。
     * @param: csid是当前不稳定的chunkserver ID
     */
    virtual void SetChunkserverUnstable(ChunkServerID csid);

    /**
     * 向map中添加对应chunkserver的copyset信息
     * @param: csid为当前chunkserverid
     * @param: cpid为当前copyset的id信息
     */
    virtual void AddCopysetIDInfo(ChunkServerID csid,
                                  const CopysetIDInfo& cpid);

    virtual void UpdateChunkserverCopysetInfo(LogicPoolID lpid,
                                              const CopysetInfo& cpinfo);

    void UpdateFileInfo(const FInfo& fileInfo) {
        fileInfo_ = fileInfo;
    }

    const FInfo* GetFileInfo() const {
        return &fileInfo_;
    }

    uint64_t GetLatestFileSn() const {
        return fileInfo_.seqnum;
    }

    void SetLatestFileSn(uint64_t newSn) {
        fileInfo_.seqnum = newSn;
    }

    FileStatus GetLatestFileStatus() const { return fileInfo_.filestatus; }

    void SetLatestFileStatus(FileStatus status) {
        fileInfo_.filestatus = status;
    }

    bool IsCloneFile() const {
        return fileInfo_.filestatus == FileStatus::CloneMetaInstalled;
    }

    /**
     * 获取对应的copyset的LeaderMayChange标志
     */
    virtual bool IsLeaderMayChange(LogicPoolID logicpoolId,
                                   CopysetID copysetId);

    /**
     * 测试使用
     * 获取copysetinfo信息
     */
    virtual CopysetInfo GetCopysetinfo(LogicPoolID lpid, CopysetID csid);

    UnstableHelper& GetUnstableHelper() {
        return unstableHelper_;
    }

    uint64_t InodeId() const {
        return fileInfo_.id;
    }

    /**
     * @brief Get file segment info about the segmentIndex
     */
    FileSegment* GetFileSegment(SegmentIndex segmentIndex);

    /**
     * @brief Clean chunks of this segment
     */
    virtual void CleanChunksInSegment(SegmentIndex segmentIndex);

 private:
    /**
     * @brief 从mds更新copyset复制组信息
     * @param logicPoolId 逻辑池id
     * @param copysetId 复制组id
     * @return 0 成功 / -1 失败
     */
    int UpdateCopysetInfoFromMDS(LogicPoolID logicPoolId, CopysetID copysetId);

    /**
     * 更新copyset的leader信息
     * @param[in]: logicPoolId逻辑池信息
     * @param[in]: copysetId复制组信息
     * @param[out]: toupdateCopyset为metacache中待更新的copyset信息指针
     */
    int UpdateLeaderInternal(LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             CopysetInfo* toupdateCopyset,
                             FileMetric* fm = nullptr);

    /**
     * 从mds拉去复制组信息，如果当前leader在复制组中
     * 则更新本地缓存，反之则不更新
     * @param: logicPoolId 逻辑池id
     * @param: copysetId 复制组id
     * @param: leaderAddr 当前的leader address
     */
    void UpdateCopysetInfoIfMatchCurrentLeader(
        LogicPoolID logicPoolId,
        CopysetID copysetId,
        const ChunkServerAddr& leaderAddr);

 private:
    MDSClient*          mdsclient_;
    MetaCacheOption   metacacheopt_;

    // chunkindex到chunkidinfo的映射表
    CURVE_CACHELINE_ALIGNMENT ChunkIndexInfoMap     chunkindex2idMap_;

    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4Segments_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<SegmentIndex, FileSegment> segments_;  // NOLINT

    // logicalpoolid和copysetid到copysetinfo的映射表
    CURVE_CACHELINE_ALIGNMENT CopysetInfoMap        lpcsid2CopsetInfoMap_;

    // chunkid到chunkidinfo的映射表
    CURVE_CACHELINE_ALIGNMENT ChunkInfoMap          chunkid2chunkInfoMap_;

    // 三个读写锁分别保护上述三个映射表
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4chunkInfoMap_;
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4ChunkInfo_;
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4CopysetInfo_;

    // chunkserverCopysetIDMap_存放当前chunkserver到copyset的映射
    // 当rpc closure设置SetChunkserverUnstable时，会设置该chunkserver
    // 的所有copyset处于leaderMayChange状态，后续copyset需要判断该值来看
    // 是否需要刷新leader

    // chunkserverid到copyset的映射
    std::unordered_map<ChunkServerID, std::set<CopysetIDInfo>> chunkserverCopysetIDMap_;  // NOLINT
    // 读写锁保护unStableCSMap
    CURVE_CACHELINE_ALIGNMENT RWLock    rwlock4CSCopysetIDMap_;

    // 当前文件信息
    FInfo fileInfo_;

    UnstableHelper unstableHelper_;
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_METACACHE_H_
