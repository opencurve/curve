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
    uint32_t    commitindex_;

    CopysetPeerInfo():chunkserverid_(0),
                    commitindex_(0) {
    }

    CopysetPeerInfo& operator=(const CopysetPeerInfo& other) {
        this->chunkserverid_ = other.chunkserverid_;
        this->peerid_ = other.peerid_;
        this->commitindex_ = other.commitindex_;
        return *this;
    }
} CopysetPeerInfo_t;

typedef struct CopysetInfo {
    std::vector<CopysetPeerInfo_t> csinfos_;
    uint16_t    leaderindex_;
    SpinLock    spinlock_;

    CopysetInfo():leaderindex_(0) {
        csinfos_.clear();
    }

    ~CopysetInfo() {
        csinfos_.clear();
        leaderindex_ = 0;
    }

    CopysetInfo& operator=(const CopysetInfo& other) {
        this->csinfos_.assign(other.csinfos_.begin(), other.csinfos_.end());
        this->leaderindex_ = other.leaderindex_;
        return *this;
    }

    CopysetInfo(const CopysetInfo& other) {
        this->csinfos_.assign(other.csinfos_.begin(), other.csinfos_.end());
        this->leaderindex_ = other.leaderindex_;
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

    void UpdateLeaderAndGetChunkserverID(ChunkServerID* chunkserverid,
                                        const EndPoint& ep) {
        spinlock_.Lock();
        leaderindex_ = 0;
        for (auto iter : csinfos_) {
            if (iter.peerid_.addr == ep) {
                *chunkserverid = iter.chunkserverid_;
                break;
            }
            leaderindex_++;
        }
        spinlock_.UnLock();
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
