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
 * File Created: Monday, 15th October 2018 10:52:48 am
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_METACACHE_STRUCT_H_
#define SRC_CLIENT_METACACHE_STRUCT_H_

#include <atomic>
#include <string>
#include <list>
#include <map>
#include <vector>
#include <unordered_map>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/spinlock.h"

using curve::common::SpinLock;

namespace curve {
namespace client {

static inline bool
operator==(const ChunkServerAddr& addr1, const ChunkServerAddr& addr2) {
    return (addr1.addr_ == addr2.addr_);
}

// copyset内的chunkserver节点的基本信息
// 包含当前chunkserver的id信息，以及chunkserver的地址信息
typedef struct CURVE_CACHELINE_ALIGNMENT CopysetPeerInfo {
    // 当前chunkserver节点的ID
    ChunkServerID chunkserverid_;
    // 当前chunkserver节点的地址信息
    ChunkServerAddr      csaddr_;

    CopysetPeerInfo():chunkserverid_(0) {
    }

    CopysetPeerInfo(ChunkServerID cid, ChunkServerAddr addr_) {
        this->chunkserverid_ = cid;
        this->csaddr_ = addr_;
    }

    CopysetPeerInfo& operator=(const CopysetPeerInfo& other) {
        this->chunkserverid_ = other.chunkserverid_;
        this->csaddr_ = other.csaddr_;
        return *this;
    }

    bool operator==(const CopysetPeerInfo& other) {
        return csaddr_ == other.csaddr_;
    }
} CopysetPeerInfo_t;

// copyset的基本信息，包含peer信息、leader信息、appliedindex信息
typedef struct CURVE_CACHELINE_ALIGNMENT CopysetInfo {
    // leader存在变更可能标志位
    bool leaderMayChange_;
    // 当前copyset的节点信息
    std::vector<CopysetPeerInfo_t> csinfos_;
    // 当前节点的apply信息，在read的时候需要，用来避免读IO进入raft
    std::atomic<uint64_t> lastappliedindex_{0};
    // leader在本copyset信息中的索引，用于后面避免重复尝试同一个leader
    int16_t     leaderindex_;
    // 当前copyset的id信息
    CopysetID   cpid_;
    // 用于保护对copyset信息的修改
    SpinLock    spinlock_;

    CopysetInfo() {
        csinfos_.clear();
        leaderindex_ = -1;
        lastappliedindex_ = 0;
        leaderMayChange_ = false;
    }

    ~CopysetInfo() {
        csinfos_.clear();
        leaderindex_ = -1;
    }

    CopysetInfo& operator=(const CopysetInfo& other) {
        this->cpid_ = other.cpid_;
        this->csinfos_.assign(other.csinfos_.begin(), other.csinfos_.end());
        this->leaderindex_ = other.leaderindex_;
        this->lastappliedindex_.store(other.lastappliedindex_);
        this->leaderMayChange_ = other.leaderMayChange_;
        return *this;
    }

    CopysetInfo(const CopysetInfo& other)
        : leaderMayChange_(other.leaderMayChange_),
          csinfos_(other.csinfos_),
          lastappliedindex_(other.lastappliedindex_.load()),
          leaderindex_(other.leaderindex_),
          cpid_(other.cpid_) {}

    uint64_t GetAppliedIndex() const {
        return lastappliedindex_.load(std::memory_order_acquire);
    }

    void SetLeaderUnstableFlag() {
        leaderMayChange_ = true;
    }

    void ResetSetLeaderUnstableFlag() {
        leaderMayChange_ = false;
    }

    bool LeaderMayChange() {
        return leaderMayChange_;
    }

    /**
     * read,write返回时，会携带最新的appliedindex更新当前的appliedindex
     * 如果read，write失败，那么会将appliedindex更新为0
     * @param: appliedindex为待更新的值
     */
    void UpdateAppliedIndex(uint64_t appliedindex) {
        uint64_t curIndex = lastappliedindex_.load(std::memory_order_acquire);

        if (appliedindex != 0 && appliedindex <= curIndex) {
            return;
        }

        while (!lastappliedindex_.compare_exchange_strong(
            curIndex, appliedindex, std::memory_order_acq_rel)) {
            if (curIndex >= appliedindex) {
                break;
            }
        }
    }

    /**
     * 获取当前leader的索引
     */
    int16_t GetCurrentLeaderIndex() {
        return leaderindex_;
    }

    bool GetCurrentLeaderServerID(ChunkServerID* id) {
        if (leaderindex_ >= 0) {
            if (csinfos_.size() < leaderindex_) {
                return false;
            } else {
                *id = csinfos_[leaderindex_].chunkserverid_;
                return true;
            }
        } else {
            return false;
        }
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

        uint16_t tempindex = 0;
        for (auto iter : csinfos_) {
            if (iter.csaddr_.addr_ == ep) {
                *chunkserverid = iter.chunkserverid_;
                break;
            }
            tempindex++;
        }
        leaderindex_ = tempindex;

        if (leaderindex_ >= csinfos_.size() && newleader) {
            /**
             * client在接收到RPC返回redirect回复的时候会更新metacache的leader信息，
             * 如果返回的redirect chunkserver不在这个metacache里，则直接将这个新的
             * leader插入到metacache中。
             */
            ChunkServerAddr pd(ep);
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
     * 如果leader的addr变更了，那么要变更chunkserverid到addr的映射
     * @param: id为新leader的id，如果该id在copyset中则更新其addr，
     *          如果不在就插入到copyset
     * @param: addr为新的leader的地址信息
     */
    int UpdateLeaderInfo(ChunkServerID id, const ChunkServerAddr& addr) {
        spinlock_.Lock();
        bool exists = false;
        uint16_t tempindex = 0;
        for (auto iter : csinfos_) {
            if (iter.csaddr_ == addr) {
                exists = true;
                break;
            }
            tempindex++;
        }

        // 新的addr不在当前copyset内，如果其id合法，那么将其插入copyset
        if (!exists && id) {
            csinfos_.push_back(CopysetPeerInfo(id, addr));
        } else if (exists == false) {
            LOG(WARNING) << addr.ToString() << " not in current copyset and "
                         << "its chunkserverid is not valid " << id;
            spinlock_.UnLock();
            return -1;
        }
        leaderindex_ = tempindex;
        spinlock_.UnLock();
        return 0;
    }

    /**
     * 获取leader信息
     * @param: chunkserverid是出参
     * @param: ep是出参
     */
    int GetLeaderInfo(ChunkServerID* chunkserverid, EndPoint* ep) {
        // 第一次获取leader,如果当前leader信息没有确定，返回-1，由外部主动发起更新leader
        if (leaderindex_ < 0 || leaderindex_ >= csinfos_.size()) {
            return -1;
        }

        *chunkserverid = csinfos_[leaderindex_].chunkserverid_;
        *ep = csinfos_[leaderindex_].csaddr_.addr_;
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
        return !csinfos_.empty();
    }

    /**
     * 更新leaderindex
     */
    void UpdateLeaderIndex(int index) {
        leaderindex_ = index;
    }

    /**
     * 当前copyset是否存在对应的chunkserver address
     * @param: addr需要检测的chunkserver
     * @return: true存在；false不存在
     */
    bool HasChunkServerInCopyset(const ChunkServerAddr& addr) const {
        for (const auto& peer : csinfos_) {
            if (peer.csaddr_ == addr) {
                return true;
            }
        }

        return false;
    }
} CopysetInfo_t;

typedef struct CopysetIDInfo {
    LogicPoolID lpid;
    CopysetID   cpid;

    CopysetIDInfo(LogicPoolID logicpoolid, CopysetID copysetid) {
        lpid = logicpoolid;
        cpid = copysetid;
    }

    CopysetIDInfo(const CopysetIDInfo& other) {
        lpid = other.lpid;
        cpid = other.cpid;
    }

    CopysetIDInfo& operator=(const CopysetIDInfo& other) {
        lpid = other.lpid;
        cpid = other.cpid;
        return *this;
    }
} CopysetIDInfo_t;

static inline bool
operator<(const CopysetIDInfo& cpidinfo1, const CopysetIDInfo& cpidinfo2) {
        return cpidinfo1.lpid <= cpidinfo2.lpid &&
               cpidinfo1.cpid < cpidinfo2.cpid;
}

static inline bool
operator==(const CopysetIDInfo& cpidinfo1, const CopysetIDInfo& cpidinfo2) {
    return cpidinfo1.cpid == cpidinfo2.cpid &&
           cpidinfo1.lpid == cpidinfo2.lpid;
}

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_METACACHE_STRUCT_H_
