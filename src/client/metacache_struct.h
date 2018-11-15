/*
 * Project: curve
 * File Created: Monday, 15th October 2018 10:52:48 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_METACACHE_STRUCT_H
#define CURVE_LIBCURVE_METACACHE_STRUCT_H

#include <string>
#include <list>
#include <map>
#include <vector>
#include <unordered_map>

#include "src/client/client_common.h"
#include "src/common/spinlock.h"

using curve::common::SpinLock;

namespace curve {
namespace client {

typedef struct CopysetIDInfo {
    CopysetID       copysetid_;
    LogicPoolID     logicpoolid_;
    CopysetIDInfo() {
        logicpoolid_ = 0;
        copysetid_ = 0;
    }

    CopysetIDInfo(LogicPoolID lpid, CopysetID csid) {
        logicpoolid_ = lpid;
        copysetid_ = csid;
    }
} CopysetIDInfo_t;

typedef struct Chunkinfo : public CopysetIDInfo {
    ChunkID         chunkid_;
    Chunkinfo() {
        chunkid_ = 0;
        logicpoolid_ = 0;
        copysetid_ = 0;
    }

    Chunkinfo(const Chunkinfo& chunkinfo) {
        this->chunkid_ = chunkinfo.chunkid_;
        this->logicpoolid_ = chunkinfo.logicpoolid_;
        this->copysetid_ = chunkinfo.copysetid_;
    }

    Chunkinfo& operator=(const Chunkinfo& chunkinfo) {
        this->chunkid_ = chunkinfo.chunkid_;
        this->logicpoolid_ = chunkinfo.logicpoolid_;
        this->copysetid_ = chunkinfo.copysetid_;
        return *this;
    }
} Chunkinfo_t;

typedef struct CopysetPeerInfo {
    ChunkServerID chunkserverid_;
    PeerId      peerid_;

    CopysetPeerInfo():chunkserverid_(0) {
    }

    CopysetPeerInfo(ChunkServerID cid, PeerId pid) {
        this->chunkserverid_ = cid;
        this->peerid_ = pid;
    }

    CopysetPeerInfo& operator=(const CopysetPeerInfo& other) {
        this->chunkserverid_ = other.chunkserverid_;
        this->peerid_ = other.peerid_;
        return *this;
    }
} CopysetPeerInfo_t;

typedef struct CopysetInfo {
    std::vector<CopysetPeerInfo_t> csinfos_;
    uint32_t    lastappliedindex_;
    uint16_t    leaderindex_;
    SpinLock    spinlock_;

    CopysetInfo() {
        csinfos_.clear();
        leaderindex_ = 0;
        lastappliedindex_ = 0;
    }

    ~CopysetInfo() {
        csinfos_.clear();
        leaderindex_ = 0;
    }

    CopysetInfo& operator=(const CopysetInfo& other) {
        this->csinfos_.assign(other.csinfos_.begin(), other.csinfos_.end());
        this->leaderindex_ = other.leaderindex_;
        this->lastappliedindex_ = other.lastappliedindex_;
        return *this;
    }

    CopysetInfo(const CopysetInfo& other) {
        this->csinfos_.assign(other.csinfos_.begin(), other.csinfos_.end());
        this->leaderindex_ = other.leaderindex_;
        this->lastappliedindex_ = other.lastappliedindex_;
    }

    uint64_t GetAppliedIndex() {
        return lastappliedindex_;
    }

    void UpdateAppliedIndex(uint64_t appliedindex) {
        lastappliedindex_ = appliedindex;
    }

    void ChangeLeaderID(const PeerId& leaderid) {
        spinlock_.Lock();
        leaderindex_ = 0;
        for (auto iter : csinfos_) {
            if (iter.peerid_ == leaderid) {
                break;
            }
            leaderindex_++;
        }
        spinlock_.UnLock();
    }

    int UpdateLeaderAndGetChunkserverID(ChunkServerID* chunkserverid,
                                    const EndPoint& ep, bool newleader = true) {
        spinlock_.Lock();
        leaderindex_ = 0;
        for (auto iter : csinfos_) {
            if (iter.peerid_.addr == ep) {
                *chunkserverid = iter.chunkserverid_;
                break;
            }
            leaderindex_++;
        }
        if (leaderindex_ >= csinfos_.size() && newleader) {
            /**
             * the new leader addr not in the copyset
             * current, we just push the new leader into copyset info
             * later, if chunkserver can not get leader, we will get 
             * latest copyset info
             */
            PeerId pd(ep);
            csinfos_.push_back(CopysetPeerInfo(*chunkserverid, pd));
            spinlock_.UnLock();
            // TODO(tongguangxun): pull latest copyset info
            return 0;
        }

        if (leaderindex_ < csinfos_.size()) {
            spinlock_.UnLock();
            return 0;
        }
        spinlock_.UnLock();
        return -1;
    }

    int GetLeaderInfo(ChunkServerID* chunkserverid, EndPoint* ep) {
        *chunkserverid = csinfos_[leaderindex_].chunkserverid_;
        *ep = csinfos_[leaderindex_].peerid_.addr;
        return 0;
    }

    void AddCopysetPeerInfo(const CopysetPeerInfo& csinfo) {
        spinlock_.Lock();
        csinfos_.push_back(csinfo);
        spinlock_.UnLock();
    }

    bool IsValid() {
        return leaderindex_ != -1 && !csinfos_.empty();
    }
} CopysetInfo_t;

}   // namespace client
}   // namespace curve

#endif  // CURVE_LIBCURVE_METACACHE_STRUCT_H
