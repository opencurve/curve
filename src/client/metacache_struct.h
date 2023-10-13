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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/common/bitmap.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/spinlock.h"

namespace curve {
namespace client {

using curve::common::Bitmap;
using curve::common::BthreadRWLock;
using curve::common::ReadLockGuard;
using curve::common::SpinLock;
using curve::common::WriteLockGuard;

// Basic information of chunkserver nodes in the copyset
// Contains the ID information of the current chunkserver and the address
// information of the chunkserver
template <typename T>
struct CopysetPeerInfo {
    // The ID of the current chunkserver node
    T peerID = 0;
    // The internal address of the current chunkserver node
    PeerAddr internalAddr;
    // The external address of the current chunkserver node
    PeerAddr externalAddr;

    CopysetPeerInfo() = default;

    CopysetPeerInfo(const T& cid, const PeerAddr& internal,
                    const PeerAddr& external)
        : peerID(cid), internalAddr(internal), externalAddr(external) {}

    bool operator==(const CopysetPeerInfo& other) const {
        return this->internalAddr == other.internalAddr &&
               this->externalAddr == other.externalAddr;
    }

    bool IsEmpty() const {
        return this->peerID == 0 && this->internalAddr.IsEmpty() &&
               this->externalAddr.IsEmpty();
    }
};

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const CopysetPeerInfo<T>& c) {
    os << "peer id : " << c.peerID
       << ", internal address : " << c.internalAddr.ToString()
       << ", external address : " << c.externalAddr.ToString();

    return os;
}

// copyset's informations including peer and leader information
template <typename T>
struct CURVE_CACHELINE_ALIGNMENT CopysetInfo {
    // Possible flag bits for leader changes
    bool leaderMayChange_ = false;
    // Node information of the current copyset
    std::vector<CopysetPeerInfo<T>> csinfos_;
    // The index of the leader in this copyset information is used to avoid
    // repeated attempts at the same leader in the future
    int16_t leaderindex_ = -1;
    // The ID information of the current copyset
    CopysetID cpid_ = 0;
    LogicPoolID lpid_ = 0;
    // Used to protect modifications to copyset information
    SpinLock spinlock_;

    CopysetInfo() = default;
    ~CopysetInfo() = default;

    CopysetInfo& operator=(const CopysetInfo& other) {
        this->cpid_ = other.cpid_;
        this->lpid_ = other.lpid_;
        this->csinfos_ = other.csinfos_;
        this->leaderindex_ = other.leaderindex_;
        this->leaderMayChange_ = other.leaderMayChange_;
        return *this;
    }

    CopysetInfo(const CopysetInfo& other)
        : leaderMayChange_(other.leaderMayChange_),
          csinfos_(other.csinfos_),
          leaderindex_(other.leaderindex_),
          cpid_(other.cpid_),
          lpid_(other.lpid_) {}

    CopysetInfo(CopysetInfo&& other) noexcept
        : leaderMayChange_(other.leaderMayChange_),
          csinfos_(std::move(other.csinfos_)),
          leaderindex_(other.leaderindex_),
          cpid_(other.cpid_),
          lpid_(other.lpid_) {}

    CopysetInfo& operator=(CopysetInfo&& other) noexcept {
        using std::swap;

        swap(leaderMayChange_, other.leaderMayChange_);
        swap(csinfos_, other.csinfos_);
        swap(leaderindex_, other.leaderindex_);
        swap(cpid_, other.cpid_);
        swap(lpid_, other.lpid_);

        return *this;
    }

    void SetLeaderUnstableFlag() { leaderMayChange_ = true; }

    void ResetSetLeaderUnstableFlag() { leaderMayChange_ = false; }

    bool LeaderMayChange() const { return leaderMayChange_; }

    bool HasValidLeader() const {
        return !leaderMayChange_ && leaderindex_ >= 0 &&
               leaderindex_ < csinfos_.size();
    }

    /**
     * Get the index of the current leader
     */
    int16_t GetCurrentLeaderIndex() const { return leaderindex_; }

    bool GetCurrentLeaderID(T* id) const {
        if (leaderindex_ >= 0) {
            if (static_cast<int>(csinfos_.size()) < leaderindex_) {
                return false;
            } else {
                *id = csinfos_[leaderindex_].peerID;
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * Update the leaderindex, if the leader is not in the current configuration
     * group, return -1
     * @param: addr is the address information of the new leader
     */
    int UpdateLeaderInfo(const PeerAddr& addr,
                         CopysetPeerInfo<T> csInfo = CopysetPeerInfo<T>()) {
        VLOG(3) << "update leader info, pool " << lpid_ << ", copyset " << cpid_
                << ", current leader " << addr.ToString();

        spinlock_.Lock();
        bool exists = false;
        uint16_t tempindex = 0;
        for (auto iter : csinfos_) {
            if (iter.internalAddr == addr || iter.externalAddr == addr) {
                exists = true;
                break;
            }
            tempindex++;
        }

        // The new addr is not within the current copyset. If csInfo is not
        // empty, insert it into the copyset
        if (!exists && !csInfo.IsEmpty()) {
            csinfos_.push_back(csInfo);
        } else if (exists == false) {
            LOG(WARNING) << addr.ToString() << " not in current copyset and "
                         << "its peer info not supplied";
            spinlock_.UnLock();
            return -1;
        }
        leaderindex_ = tempindex;
        spinlock_.UnLock();
        return 0;
    }

    /**
     * get leader info
     * @param[out]: peer id
     * @param[out]: ep
     */
    int GetLeaderInfo(T* peerid, EndPoint* ep) {
        // For the first time obtaining the leader, if the current leader
        // information is not determined, return -1, and the external initiative
        // will be initiated to update the leader
        if (leaderindex_ < 0 ||
            leaderindex_ >= static_cast<int>(csinfos_.size())) {
            LOG(INFO) << "GetLeaderInfo pool " << lpid_ << ", copyset " << cpid_
                      << " has no leader";

            return -1;
        }

        *peerid = csinfos_[leaderindex_].peerID;
        *ep = csinfos_[leaderindex_].externalAddr.addr_;

        VLOG(3) << "GetLeaderInfo pool " << lpid_ << ", copyset " << cpid_
                << " leader id " << *peerid << ", end point "
                << butil::endpoint2str(*ep).c_str();

        return 0;
    }

    /**
     * Add peerinfo for copyset
     * @param: csinfo is the peer information to be added
     */
    void AddCopysetPeerInfo(const CopysetPeerInfo<T>& csinfo) {
        spinlock_.Lock();
        csinfos_.push_back(csinfo);
        spinlock_.UnLock();
    }

    /**
     * Is the current CopysetInfo legal
     */
    bool IsValid() const { return !csinfos_.empty(); }

    /**
     * Update leaderindex
     */
    void UpdateLeaderIndex(int index) { leaderindex_ = index; }

    /**
     * Does the current copyset have a corresponding chunkserver address
     * @param: addr Chunkserver to be detected
     * @return: true exists; False does not exist
     */
    bool HasPeerInCopyset(const PeerAddr& addr) const {
        for (const auto& peer : csinfos_) {
            if (peer.internalAddr == addr || peer.externalAddr == addr) {
                return true;
            }
        }

        return false;
    }
};

template <typename T>
inline std::ostream& operator<<(std::ostream& os,
                                const CopysetInfo<T>& copyset) {
    os << "pool id : " << copyset.lpid_ << ", copyset id : " << copyset.cpid_
       << ", leader index : " << copyset.leaderindex_
       << ", leader may change : " << copyset.leaderMayChange_ << ", peers : ";

    for (auto& p : copyset.csinfos_) {
        os << p << " ";
    }

    return os;
}

struct CopysetIDInfo {
    LogicPoolID lpid = 0;
    CopysetID cpid = 0;

    CopysetIDInfo(LogicPoolID logicpoolid, CopysetID copysetid)
        : lpid(logicpoolid), cpid(copysetid) {}
};

inline bool operator<(const CopysetIDInfo& cpidinfo1,
                      const CopysetIDInfo& cpidinfo2) {
    return cpidinfo1.lpid <= cpidinfo2.lpid && cpidinfo1.cpid < cpidinfo2.cpid;
}

inline bool operator==(const CopysetIDInfo& cpidinfo1,
                       const CopysetIDInfo& cpidinfo2) {
    return cpidinfo1.cpid == cpidinfo2.cpid && cpidinfo1.lpid == cpidinfo2.lpid;
}

class FileSegment {
 public:
    FileSegment(SegmentIndex segmentIndex, uint32_t segmentSize,
                uint32_t discardGranularity)
        : segmentIndex_(segmentIndex),
          segmentSize_(segmentSize),
          discardGranularity_(discardGranularity),
          rwlock_(),
          discardBitmap_(segmentSize_ / discardGranularity_),
          chunks_() {}

    /**
     * @brief Confirm if all bit was discarded
     * @return Return true if if all bits are set, otherwise return false
     */
    bool IsAllBitSet() const {
        return discardBitmap_.NextClearBit(0) == curve::common::Bitmap::NO_POS;
    }

    void AcquireReadLock() { rwlock_.RDLock(); }

    void AcquireWriteLock() { rwlock_.WRLock(); }

    void ReleaseLock() { rwlock_.Unlock(); }

    /**
     * @brief Get internal bitmap for unit-test
     * @return Internal bitmap
     */
    Bitmap& GetBitmap() { return discardBitmap_; }

    void SetBitmap(const uint64_t offset, const uint64_t length);
    void ClearBitmap(const uint64_t offset, const uint64_t length);

    void ClearBitmap() { discardBitmap_.Clear(); }

 private:
    const SegmentIndex segmentIndex_;
    const uint32_t segmentSize_;
    const uint32_t discardGranularity_;
    BthreadRWLock rwlock_;
    Bitmap discardBitmap_;
    std::unordered_map<ChunkIndex, ChunkIDInfo> chunks_;
};

inline void FileSegment::SetBitmap(const uint64_t offset,
                                   const uint64_t length) {
    if (length < discardGranularity_) {
        return;
    }

    if (offset == 0 && length == segmentSize_) {
        return discardBitmap_.Set();
    }

    auto res = std::div(static_cast<int64_t>(offset),
                        static_cast<int64_t>(discardGranularity_));
    uint32_t startIndex = res.quot;
    if (res.rem != 0) {
        ++startIndex;
    }

    uint32_t endIndex = (offset + length) / discardGranularity_ - 1;

    return discardBitmap_.Set(startIndex, endIndex);
}

inline void FileSegment::ClearBitmap(const uint64_t offset,
                                     const uint64_t length) {
    if (offset == 0 && length == segmentSize_) {
        return discardBitmap_.Clear();
    }

    uint32_t startIndex = offset / discardGranularity_;
    auto res = std::div(static_cast<int64_t>(offset + length),
                        static_cast<int64_t>(discardGranularity_));

    uint32_t endIndex = res.quot;
    if (res.rem == 0 && endIndex != 0) {
        --endIndex;
    }

    return discardBitmap_.Clear(startIndex, endIndex);
}

enum class FileSegmentLockType { Read, Write };

template <FileSegmentLockType type>
class FileSegmentLockGuard {
 public:
    explicit FileSegmentLockGuard(FileSegment* segment) : segment_(segment) {
        Lock();
    }

    FileSegmentLockGuard(const FileSegmentLockGuard&) = delete;
    FileSegmentLockGuard& operator=(const FileSegmentLockGuard&) = delete;

    ~FileSegmentLockGuard() { UnLock(); }

    void Lock() {
        if (type == FileSegmentLockType::Read) {
            segment_->AcquireReadLock();
        } else {
            segment_->AcquireWriteLock();
        }
    }

    void UnLock() { segment_->ReleaseLock(); }

 private:
    FileSegment* segment_;
};

using FileSegmentReadLockGuard =
    FileSegmentLockGuard<FileSegmentLockType::Read>;

using FileSegmentWriteLockGuard =
    FileSegmentLockGuard<FileSegmentLockType::Write>;

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_METACACHE_STRUCT_H_
