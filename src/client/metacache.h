/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:22 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_METACACHE_H
#define CURVE_LIBCURVE_METACACHE_H

#include <string>
#include <list>
#include <map>
#include <vector>
#include <unordered_map>

#include "src/common/spinlock.h"
#include "src/client/session.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"

using curve::common::SpinLock;

namespace curve {
namespace client {

class MetaCache {
 public:
    using CopysetLogicPoolIDStr =
            std::string;
    using ChunkIndexInfoMap =
            std::map<ChunkIndex, Chunkinfo_t>;
    using CopysetIDInfoMap =
            std::unordered_map<ChunkID, CopysetIDInfo_t>;
    using CopysetInfoMap =
            std::unordered_map<CopysetLogicPoolIDStr, CopysetInfo_t>;

    explicit MetaCache(Session* session);
    CURVE_MOCK ~MetaCache();

    CURVE_MOCK int GetChunkInfo(ChunkIndex chunkidx, Chunkinfo_t* chunkinfo);
    /**
     * refresh: false, fetchback
     *          true, fetch through
     * ret:  0, sucess
     *      -1, failes
     */
    CURVE_MOCK int GetLeader(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkServerID* serverId,
                  butil::EndPoint* serverAddr,
                  bool refresh = false);
    /**
     *  update copyset's leader address，return leader's chunkserver id
     *  ret: -1, failed
     *        0, success
     */
    CURVE_MOCK int UpdateLeader(LogicPoolID logicPoolId,
                    CopysetID copysetId,
                    ChunkServerID* leaderId,
                    const butil::EndPoint &leaderAddr);

    static inline std::string LogicPoolCopysetID2Str(LogicPoolID lpid,
                                                    CopysetID csid) {
        return std::to_string(lpid).append("_").append(std::to_string(csid));
    }

    void UpdateChunkInfo(ChunkIndex cindex, Chunkinfo_t chunkinfo);
    void UpdateCopysetIDInfo(ChunkID cid,
                                        LogicPoolID logicPoolId,
                                        CopysetID copysetId);
    void UpdateCopysetInfo(LogicPoolID logicPoolId,
                                            CopysetID copysetId,
                                            CopysetInfo_t cslist);

    CopysetInfo_t GetServerList(LogicPoolID logicPoolId, CopysetID copysetId);

 private:
        int GetLeader(const LogicPoolID &logicPoolId,
                                const CopysetID &copysetId,
                                const Configuration &conf,
                                PeerId *leaderId);

 private:
    Session*    session_;

    ChunkIndexInfoMap CURVE_CACHELINE_ALIGNMENT chunkindex2idMap_;
    CopysetIDInfoMap CURVE_CACHELINE_ALIGNMENT  chunkid2cslpMap_;
    CopysetInfoMap CURVE_CACHELINE_ALIGNMENT  lpcsid2serverlistMap_;
    SpinLock CURVE_CACHELINE_ALIGNMENT   spinlock4ChunkInfo_;
    SpinLock CURVE_CACHELINE_ALIGNMENT   spinlock4CopysetIDInfo_;
    SpinLock CURVE_CACHELINE_ALIGNMENT   spinlock4CopysetInfo_;
};
}   // namespace client
}   // namespace curve
#endif   // !CURVE_LIBCURVE_METACACHE_H
