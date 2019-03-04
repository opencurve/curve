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

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/common/spinlock.h"

using curve::common::SpinLock;

namespace curve {
namespace client {
// copyset内的peer的基本信息
typedef struct CURVE_CACHELINE_ALIGNMENT CopysetPeerInfo {
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

// copyset的基本信息，包含peer信息、leader信息、appliedindex信息
typedef struct CURVE_CACHELINE_ALIGNMENT CopysetInfo {
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

    /**
     * read,write返回时，会携带最新的appliedindex更新当前的appliedindex
     * 如果read，write失败，那么会将appliedindex更新为0
     * @param: appliedindex为待更新的值
     */
    void UpdateAppliedIndex(uint64_t appliedindex) {
        spinlock_.Lock();
        if (appliedindex == 0 || appliedindex > lastappliedindex_)
            lastappliedindex_ = appliedindex;
        spinlock_.UnLock();
    }

    /**
     * 更改当前copyset的leader
     * @param: leaderid为新的leader
     */
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

    /**
     * 更新leader信息，并且获取其chunkserverID
     * @param: chunkserverid是出参，为要获取的chunkserver信息
     * @param: ep是当前leader的地址信息
     * @param: newleader是否将新的leader信息push到copysetinfo中
     * @return: 成功返回0，否则返回-1
     */
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

    /**
     * 获取leader信息
     * @param: chunkserverid是出参
     * @param: ep是出参
     */
    int GetLeaderInfo(ChunkServerID* chunkserverid, EndPoint* ep) {
        *chunkserverid = csinfos_[leaderindex_].chunkserverid_;
        *ep = csinfos_[leaderindex_].peerid_.addr;
        return 0;
    }

    /**
     * 添加copyset的peerinfo
     * @param: csinfo为待添加的peer信息
     */
    void AddCopysetPeerInfo(const CopysetPeerInfo& csinfo) {
        spinlock_.Lock();
        csinfos_.push_back(csinfo);
        spinlock_.UnLock();
    }

    /**
     * 当前CopysetInfo是否合法
     */
    bool IsValid() {
        return leaderindex_ != -1 && !csinfos_.empty();
    }
} CopysetInfo_t;

}   // namespace client
}   // namespace curve

#endif  // CURVE_LIBCURVE_METACACHE_STRUCT_H
