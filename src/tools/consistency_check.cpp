/*
 * Project: curve
 * File Created: Saturday, 29th June 2019 12:39:40 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>

#include "src/tools/consistency_check.h"

DEFINE_string(filename, "", "filename to check consistency");
DEFINE_bool(check_hash, true, R"(用户需要先确认copyset的applyindex一致之后
                        再去查copyset内容是不是一致。通常需要先设置
                        check_hash = false先检查copyset的applyindex是否一致
                        如果一致了再设置check_hash = true，
                        检查copyset内容是不是一致)");
DEFINE_uint32(chunkServerBasePort, 8200, "base port of chunkserver");
DECLARE_string(mdsAddr);

namespace curve {
namespace tool {

std::ostream& operator<<(std::ostream& os, const CopySet& copyset) {
    os << "logicPoolId = " << copyset.first
       << ",copysetId = " << copyset.second;
    return os;
}

std::ostream& operator<<(std::ostream& os, const CsAddrsType& csAddrs) {
    std::vector<std::string> ipVec;
    std::vector<uint32_t> seqVec;
    for (uint32_t i = 0; i < csAddrs.size(); ++i) {
        std::string ip;
        uint32_t port;
        if (curve::common::NetCommon::SplitAddrToIpPort(csAddrs[i],
                                                        &ip, &port)) {
            uint32_t csSeq = port - FLAGS_chunkServerBasePort;
            ipVec.emplace_back(ip);
            seqVec.emplace_back(csSeq);
        }
    }
    os << "hosts:[";
    for (uint32_t i = 0; i < ipVec.size(); ++i) {
        if (i != 0) {
            os << ",";
        }
        os << ipVec[i];
    }
    os << "]";
    os << ",chunkservers:[";
    for (uint32_t i = 0; i < seqVec.size(); ++i) {
        if (i != 0) {
            os << ",";
        }
        os << "chunkserver" << seqVec[i];
    }
    os << "]";
    return os;
}

ConsistencyCheck::ConsistencyCheck(
                    std::shared_ptr<NameSpaceToolCore> nameSpaceToolCore,
                    std::shared_ptr<ChunkServerClient> csClient) :
                        nameSpaceToolCore_(nameSpaceToolCore),
                        csClient_(csClient),
                        inited_(false) {
}

bool ConsistencyCheck::SupportCommand(const std::string& command) {
    return (command == kCheckConsistencyCmd);
}

int ConsistencyCheck::Init() {
    if (!inited_) {
        int res = nameSpaceToolCore_->Init(FLAGS_mdsAddr);
        if (res != 0) {
            std::cout << "Init nameSpaceToolCore fail!" << std::endl;
            return -1;
        }
        inited_ = true;
    }
    return 0;
}

int ConsistencyCheck::RunCommand(const std::string &cmd) {
    if (Init() != 0) {
        std::cout << "Init ConsistencyCheck failed" << std::endl;
        return -1;
    }
    if (cmd == kCheckConsistencyCmd) {
        return CheckFileConsistency(FLAGS_filename, FLAGS_check_hash);
    } else {
        std::cout << "Command not supported!" << std::endl;
        return -1;
    }
}

int ConsistencyCheck::CheckFileConsistency(const std::string& fileName,
                                           bool checkHash) {
    std::set<CopySet> copysets;
    int res = FetchFileCopyset(fileName, &copysets);
    if (res != 0) {
        std::cout << "FetchFileCopyset of " << fileName << " fail!"
                  << std::endl;
        return -1;
    }
    for (auto copyset : copysets) {
        res = CheckCopysetConsistency(copyset, checkHash);
        if (res != 0) {
            std::cout << "CheckCopysetConsistency fail!" << std::endl;
            return -1;
        }
    }
    std::cout << "consistency check success!" << std::endl;
    return 0;
}

void ConsistencyCheck::PrintHelp(const std::string &cmd) {
    if (!SupportCommand(cmd)) {
        std::cout << "Command not supported!" << std::endl;
        return;
    }
    std::cout << "Example: " << std::endl;
    std::cout << "curve_ops_tool check-consistency -filename=/test [-check_hash=false]"  << std::endl;  // NOLINT
}

int ConsistencyCheck::FetchFileCopyset(const std::string& fileName,
                                       std::set<CopySet>* copysets) {
    std::vector<PageFileSegment> segments;
    int res = nameSpaceToolCore_->GetFileSegments(fileName, &segments);
    if (res != 0) {
        std::cout << "GetFileSegments from mds fail!" << std::endl;
        return -1;
    }
    for (const auto& segment : segments) {
        PoolIdType lpid = segment.logicalpoolid();
        for (int i = 0; i < segment.chunks_size(); ++i) {
            const auto& chunk = segment.chunks(i);
            CopySet copyset(lpid, chunk.copysetid());
            copysets->emplace(copyset);
            chunksInCopyset_[copyset].emplace(chunk.chunkid());
        }
    }
    return 0;
}

int ConsistencyCheck::CheckCopysetConsistency(
                                const CopySet copyset,
                                bool checkHash) {
    std::vector<ChunkServerLocation> csLocs;
    int res = nameSpaceToolCore_->GetChunkServerListInCopySet(
                                                copyset.first,
                                                copyset.second,
                                                &csLocs);
    if (res != 0) {
        std::cout << "GetServerList info failed, exit consistency check!"
                  << std::endl;
        return -1;
    }
    std::vector<std::string> csAddrs;
    for (const auto& csLoc : csLocs) {
        std::string hostIp = csLoc.hostip();
        uint64_t port = csLoc.port();
        std::string csAddr = hostIp + ":" + std::to_string(port);
        csAddrs.emplace_back(csAddr);
    }
    // 检查当前copyset的chunkserver内容是否一致
    if (checkHash) {
        // 先检查apply index是否一致
        res = CheckApplyIndex(copyset, csAddrs);
        if (res != 0) {
            std::cout << "Apply index not match when check hash!" << std::endl;
            return -1;
        }
        return CheckCopysetHash(copyset, csAddrs);
    } else {
        return CheckApplyIndex(copyset, csAddrs);
    }
}

int ConsistencyCheck::GetCopysetStatusResponse(
                        const std::string& csAddr,
                        const CopySet copyset,
                        CopysetStatusResponse* response) {
    int res = csClient_->Init(csAddr);
    if (res != 0) {
        std::cout << "Init chunkserverClient to " << csAddr
                  << " fail!" << std::endl;
        return -1;
    }
    CopysetStatusRequest request;
    curve::common::Peer *peer = new curve::common::Peer();
    peer->set_address(csAddr);
    request.set_logicpoolid(copyset.first);
    request.set_copysetid(copyset.second);
    request.set_allocated_peer(peer);
    request.set_queryhash(false);
    res = csClient_->GetCopysetStatus(request, response);
    if (res != 0) {
        std::cout << "GetCopysetStatus from " << csAddr
                  << " fail!" << std::endl;
        return -1;
    }
    return 0;
}

int ConsistencyCheck::CheckCopysetHash(const CopySet& copyset,
                                       const CsAddrsType& csAddrs) {
    for (const auto& chunkId : chunksInCopyset_[copyset]) {
        Chunk chunk(copyset.first, copyset.second, chunkId);
        int res = CheckChunkHash(chunk, csAddrs);
        if (res != 0) {
            std::cout << "{" << chunk
                      << "," << csAddrs << "}" << std::endl;
            return -1;
        }
    }
    return 0;
}

int ConsistencyCheck::CheckChunkHash(const Chunk& chunk,
                                     const CsAddrsType& csAddrs) {
    std::string preHash;
    std::string curHash;
    bool first = true;
    for (const auto& csAddr : csAddrs) {
        int res = csClient_->Init(csAddr);
        if (res != 0) {
            std::cout << "Init chunkserverClient to " << csAddr
                      << " fail!" << std::endl;
            return -1;
        }
        res = csClient_->GetChunkHash(chunk, &curHash);
        if (res != 0) {
            std::cout << "GetChunkHash from " << csAddr << " fail" << std::endl;
            return -1;
        }
        if (first) {
            preHash = curHash;
            first = false;
            continue;
        }
        if (curHash != preHash) {
            std::cout << "Chunk hash not equal!" << std::endl;
            std::cout << "previous chunk hash = " << preHash
                      << ", current hash = " << curHash << std::endl;
            return -1;
        }
    }
    return 0;
}

int ConsistencyCheck::CheckApplyIndex(const CopySet copyset,
                                      const CsAddrsType& csAddrs) {
    uint64_t preIndex;
    uint64_t curIndex;
    bool first = true;
    int ret = 0;
    for (const auto& csAddr : csAddrs) {
        CopysetStatusResponse response;
        int res = GetCopysetStatusResponse(csAddr, copyset, &response);
        if (res != 0) {
            std::cout << "GetCopysetStatusResponse from " << csAddr
                      << " fail" << std::endl;
            ret = -1;
            break;
        }
        curIndex = response.knownappliedindex();
        if (first) {
            preIndex = curIndex;
            first = false;
            continue;
        }
        if (curIndex != preIndex) {
            std::cout << "Apply index not equal!" << std::endl;
            std::cout << "previous apply index " << preIndex
                      << ", current index = " << curIndex << std::endl;
            ret = -1;
            break;
        }
    }
    if (ret != 0) {
        std::cout << copyset << "," << csAddrs << std::endl;
    }
    return ret;
}

}  // namespace tool
}  // namespace curve
