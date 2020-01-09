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
DECLARE_string(mdsAddr);

namespace curve {
namespace tool {

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
            copysets->emplace(CopySet(lpid, chunk.copysetid()));
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
    std::vector<std::string> copysetHash;
    std::vector<uint64_t> applyIndexVec;
    res = FetchApplyIndexOrHash(copyset, csLocs, &applyIndexVec, &copysetHash);
    if (res != 0) {
        std::cout << "FetchApplyIndexOrHash fail!" << std::endl;
        return -1;
    }
    // 检查当前copyset的chunkserver内容是否一致
    if (checkHash) {
        // 先检查apply index是否一致
        res = CheckApplyIndex(copyset, applyIndexVec);
        if (res != 0) {
            std::cout << "Apply index not match when check hash!" << std::endl;
            return -1;
        }
        return CheckHash(copyset, copysetHash);
    } else {
        return CheckApplyIndex(copyset, applyIndexVec);
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
    request.set_queryhash(FLAGS_check_hash);
    res = csClient_->GetCopysetStatus(request, response);
    if (res != 0) {
        std::cout << "GetCopysetStatus from " << csAddr
                  << " fail!" << std::endl;
        return -1;
    }
    return 0;
}

int ConsistencyCheck::CheckHash(const CopySet copyset,
                        const std::vector<std::string>& copysetHash) {
    if (copysetHash.empty()) {
        std::cout << "copysetHash is empty!" << std::endl;
        return 0;
    }
    std::string hash = copysetHash.front();
    for (auto peerHash : copysetHash) {
        if (peerHash.compare(hash) != 0) {
            std::cout << "Hash not equal! previous hash = " << hash
                      << ", current hash = " << peerHash
                      << ", copyset id = " << copyset.first
                      << ", logical pool id = " << copyset.second
                      << std::endl;
            return -1;
        }
    }
    return 0;
}

int ConsistencyCheck::CheckApplyIndex(const CopySet copyset,
                        const std::vector<uint64_t>& applyIndexVec) {
    if (applyIndexVec.empty()) {
        std::cout << "applyIndexVec is empty!" << std::endl;
        return 0;
    }
    uint64_t index = applyIndexVec.front();
    for (auto applyIndex : applyIndexVec) {
        if (index != applyIndex) {
            std::cout << "Apply index not equal! previous apply index "
                      << index << ", current index = " << applyIndex
                      << ", copyset id = " << copyset.first
                      << ", logical pool id = " << copyset.second
                      << std::endl;
            return -1;
        }
    }
    return 0;
}

int ConsistencyCheck::FetchApplyIndexOrHash(
                                const CopySet& copyset,
                                const std::vector<ChunkServerLocation>& csLocs,
                                std::vector<uint64_t>* applyIndexVec,
                                std::vector<std::string>* copysetHash) {
    for (const auto& csLoc : csLocs) {
        std::string hostIp = csLoc.hostip();
        uint64_t port = csLoc.port();
        std::string csAddr = hostIp + ":" + std::to_string(port);
        CopysetStatusResponse response;
        int res = GetCopysetStatusResponse(csAddr, copyset, &response);
        if (res != 0) {
            std::cout << "GetCopysetStatusResponse from chunkserver fail"
                      << std::endl;
            return -1;
        }
        applyIndexVec->push_back(response.knownappliedindex());
        // 存储要检查的内容
        if (FLAGS_check_hash) {
            copysetHash->push_back(response.hash());
        }
    }
    return 0;
}
}  // namespace tool
}  // namespace curve
