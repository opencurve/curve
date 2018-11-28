/*
 * Project: curve
 * Created Date: Thu Oct 11 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/copyset/copyset_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <iterator>
#include <set>
#include <unordered_set>
#include <random>
#include <utility>
#include <map>


namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;

bool operator<(const Copyset& lhs, const Copyset& rhs) {
    std::ostringstream buf1;
    std::copy(lhs.replicas.begin(),
        lhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(buf1, "_"));
    std::ostringstream buf2;
    std::copy(rhs.replicas.begin(),
        rhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(buf2, "_"));
    return buf1.str() < buf2.str();
}

std::ostream& operator<<(std::ostream& out, const Copyset& rhs) {
    out << "{copyset: ";
    std::copy(rhs.replicas.begin(),
        rhs.replicas.end(),
        std::ostream_iterator<ChunkServerIdType>(out, " "));
    return out << "}";
}


std::ostream& operator<<(std::ostream& out, const ChunkServerInfo& rhs) {
    return out << "{server:"
               << rhs.id
               << ",zone:"
               << rhs.location.zoneId
               << "}";
}

bool CopysetZoneShufflePolicy::GenCopyset(const ClusterInfo& cluster,
    int numCopysets,
    std::vector<Copyset>* out) {

    std::vector<ChunkServerInfo> chunkServers = cluster.GetChunkServerInfo();
    uint32_t numReplicas = permutationPolicy_->GetReplicaNum();

    std::vector<ChunkServerInfo> replicas(numReplicas);
    int maxNum = GetMaxPermutationNum(numCopysets,
        chunkServers.size(),
        numReplicas);

    for (int i = 0; i < maxNum; i++) {
        if (!permutationPolicy_->permutation(&chunkServers)) {
            return false;
        }

        for (uint32_t i = 0; i < chunkServers.size(); i += numReplicas) {
            if (i + numReplicas > chunkServers.size()) {
                break;
            }
            std::copy(chunkServers.begin() + i,
                chunkServers.begin() + i + numReplicas,
                replicas.begin());
            Copyset copyset;
            for (auto& replica : replicas) {
                copyset.replicas.insert(replica.id);
            }
            out->emplace_back(copyset);
            if (--numCopysets == 0) {
                LOG(INFO) << "Generate copyset success : ";
                for (Copyset& copyset : *out) {
                    LOG(INFO) << copyset << "   ";
                }
                return true;
            }
        }
    }
    return false;
}

bool CopysetZoneShufflePolicy::AddNode(const ClusterInfo& cluster,
    const ChunkServerInfo& node,
    int scatterWidth,
    std::vector<Copyset>* out) {
    // TODO(xuchaojie): fix this
    return true;
}


bool CopysetZoneShufflePolicy::RemoveNode(const ClusterInfo& cluster,
    const ChunkServerInfo& node,
    const std::vector<Copyset>& css,
    std::vector<std::pair<Copyset, Copyset>>* out) {
    // TODO(xuchaojie): fix this
    return true;
}


int CopysetZoneShufflePolicy::GetMaxPermutationNum(int numCopysets,
    int num_servers,
    int num_replicas) {
    return numCopysets;
}

bool CopysetPermutationPolicy333::permutation(
    std::vector<ChunkServerInfo> *chunkServers) {
    std::vector<ChunkServerInfo> chunkServerInZone1,
                                 chunkServerInZone2,
                                 chunkServerInZone3;
    curve::mds::topology::ZoneIdType zid1, zid2, zid3;
    for (ChunkServerInfo& sv : *chunkServers) {
        if (0 == chunkServerInZone1.size() ||
            zid1 == sv.location.zoneId) {
            chunkServerInZone1.push_back(sv);
            zid1 = sv.location.zoneId;
        } else if (0 == chunkServerInZone2.size() ||
            zid2 == sv.location.zoneId) {
            chunkServerInZone2.push_back(sv);
            zid2 = sv.location.zoneId;
        } else if (0 == chunkServerInZone3.size() ||
            zid3 == sv.location.zoneId) {
            chunkServerInZone3.push_back(sv);
            zid3 = sv.location.zoneId;
        } else {
            // more than 3 zones, add err log
            LOG(ERROR) << "[CopysetPermutationPolicy333::permutation]: "
                       << "error, cluster has more than 3 zones.";
            return false;
        }
    }

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(chunkServerInZone1.begin(), chunkServerInZone1.end(), g);
    std::shuffle(chunkServerInZone2.begin(), chunkServerInZone2.end(), g);
    std::shuffle(chunkServerInZone3.begin(), chunkServerInZone3.end(), g);

    chunkServers->clear();

    for (uint32_t i = 0; i < chunkServerInZone1.size() &&
                i < chunkServerInZone2.size() &&
                i < chunkServerInZone3.size();
                i++) {
        if (i < chunkServerInZone1.size()) {
           chunkServers->push_back(chunkServerInZone1[i]);
        }
        if (i < chunkServerInZone2.size()) {
           chunkServers->push_back(chunkServerInZone2[i]);
        }
        if (i < chunkServerInZone3.size()) {
           chunkServers->push_back(chunkServerInZone3[i]);
        }
    }

    return true;
}

/**
 * @brief  N个zone中选择3个zone放置3副本的随机排列算法
 *
 *  1. 首先对所有zone的server各自进行随机排列，
 *  2. 之后按照如下顺序依次选择：
 *
 *  zone1    zone2    zone3    zone4   ...  zoneN
 *   1        2        3         4     ...    N
 *  N+1      N+2      N+3       N+4    ...   2N
 *  ...      ...      ...       ...    ...   ...
 *
 * @param chunkServers  all chunkserver in cluster.
 *
 * @reval true   exec success.
 * @reval false  exec fail.
 */
bool CopysetPermutationPolicyN33::permutation(
    std::vector<ChunkServerInfo> *chunkServers) {
    std::map<curve::mds::topology::ZoneIdType,
        std::vector<ChunkServerInfo> > csMap;
    for (ChunkServerInfo& sv : *chunkServers) {
        curve::mds::topology::ZoneIdType zid = sv.location.zoneId;
        if (csMap.find(zid) != csMap.end()) {
            csMap[zid].push_back(sv);
        } else {
            std::vector<ChunkServerInfo> temp;
            temp.push_back(sv);
            csMap[zid] = temp;
        }
    }

    if (csMap.size() < GetZoneChosenNum()) {
        LOG(ERROR) << "[CopysetPermutationPolicyN33::permutation]:"
                   << "error, cluster must has more than 3 zones";
        return false;
    }

    std::random_device rd;
    std::mt19937 g(rd());

    int minSize = 0;
    if (!csMap.empty()) {
        minSize = csMap.begin()->second.size();
    }
    for (auto& it : csMap) {
        std::shuffle(it.second.begin(), it.second.end(), g);
        if (it.second.size() <  minSize) {
            minSize = it.second.size();
        }
    }

    chunkServers->clear();

    for (int i = 0; i < minSize; i++) {
        for (auto& it : csMap) {
            chunkServers->push_back(it.second[i]);
        }
    }
    return true;
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve

