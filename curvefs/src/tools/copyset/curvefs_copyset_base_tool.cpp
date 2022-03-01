/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-11-23
 * Author: chengyi01
 */

#include "curvefs/src/tools/copyset/curvefs_copyset_base_tool.h"

DECLARE_string(copysetId);
DECLARE_string(poolId);

// used for CopysetStatusTool
DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace copyset {

uint64_t GetCopysetKey(uint64_t copysetId, uint64_t poolId) {
    return (poolId << 32) | copysetId;
}

bool CopysetInfo2CopysetStatus(
    const curvefs::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t,
             std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>>*
        key2Status) {
    bool ret = true;
    std::map<std::string,
             std::queue<curvefs::metaserver::copyset::CopysetsStatusRequest>>
        addr2Request;
    for (auto const& i : response.copysetvalues()) {
        using tmpType = curvefs::metaserver::copyset::CopysetStatusRequest;
        tmpType tmp;
        tmp.set_copysetid(i.copysetinfo().copysetid());
        tmp.set_poolid(i.copysetinfo().poolid());
        for (auto const& j : i.copysetinfo().peers()) {
            // send request to all peer
            std::string addr;
            if (!curvefs::mds::topology::SplitPeerId(j.address(), &addr)) {
                std::cerr << "copyset[" << tmp.copysetid()
                          << "] has error peerid: " << j.address() << std::endl;
                ret = false;
                break;
            }
            auto& queueRequest = addr2Request[addr];
            if (queueRequest.empty()) {
                queueRequest.push(
                    curvefs::metaserver::copyset::CopysetsStatusRequest());
            }
            *queueRequest.front().add_copysets() = tmp;
        }
    }

    FLAGS_copysetId = FLAGS_poolId = "";  // clear copysetId&poolId
    for (auto const& i : addr2Request) {
        // set host
        FLAGS_metaserverAddr = i.first;
        copyset::GetCopysetStatusTool getCopysetStatusTool("", false);
        getCopysetStatusTool.Init();
        getCopysetStatusTool.SetRequestQueue(i.second);
        auto checkRet = getCopysetStatusTool.RunCommand();
        if (checkRet < 0) {
            std::cerr << "send request to mds get error." << std::endl;
            ret = false;
        }
        const auto& copysetsStatus =
            getCopysetStatusTool.GetResponse()->status();
        auto copysets = i.second.front().copysets();
        for (int m = 0, n = 0; m < copysets.size() && n < copysetsStatus.size();
             ++m, ++n) {
            uint64_t key = (static_cast<uint64_t>(copysets[m].poolid()) << 32) |
                           copysets[m].copysetid();
            (*key2Status)[key].push_back(copysetsStatus[n]);
        }
    }
    return ret;
}

bool CopysetInfo2CopysetStatus(
    const curvefs::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t,
             std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>>*
        key2Status) {
    bool ret = true;
    std::map<std::string,
             std::queue<curvefs::metaserver::copyset::CopysetsStatusRequest>>
        addr2Request;
    for (auto const& i : response.copysetvalues()) {
        using tmpType = curvefs::metaserver::copyset::CopysetStatusRequest;
        tmpType tmp;
        tmp.set_copysetid(i.copysetinfo().copysetid());
        tmp.set_poolid(i.copysetinfo().poolid());
        for (auto const& j : i.copysetinfo().peers()) {
            // send request to all peer
            std::string addr;
            if (!curvefs::mds::topology::SplitPeerId(j.address(), &addr)) {
                std::cerr << "copyset[" << tmp.copysetid()
                          << "] has error peerid: " << j.address() << std::endl;
                ret = false;
                break;
            }
            auto& queueRequest = addr2Request[addr];
            if (queueRequest.empty()) {
                queueRequest.push(
                    curvefs::metaserver::copyset::CopysetsStatusRequest());
            }
            *queueRequest.front().add_copysets() = tmp;
        }
    }

    FLAGS_copysetId = FLAGS_poolId = "";  // clear copysetId&poolId
    for (auto const& i : addr2Request) {
        // set host
        FLAGS_metaserverAddr = i.first;
        copyset::GetCopysetStatusTool getCopysetStatusTool("", false);
        getCopysetStatusTool.Init();
        getCopysetStatusTool.SetRequestQueue(i.second);
        auto checkRet = getCopysetStatusTool.RunCommand();
        if (checkRet < 0) {
            std::cerr << "send request to metaserver (" << FLAGS_metaserverAddr
                      << ") get error." << std::endl;
            ret = false;
        }
        const auto& copysetsStatus =
            getCopysetStatusTool.GetResponse()->status();
        auto copysets = i.second.front().copysets();
        for (int m = 0, n = 0; m < copysets.size() && n < copysetsStatus.size();
             ++m, ++n) {
            uint64_t key = (static_cast<uint64_t>(copysets[m].poolid()) << 32) |
                           copysets[m].copysetid();
            (*key2Status)[key].push_back(copysetsStatus[n]);
        }
    }
    return ret;
}

bool Response2CopysetInfo(
    const curvefs::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t, std::vector<curvefs::mds::topology::CopysetValue>>*
        key2Info) {
    bool ret = true;
    for (auto const& i : response.copysetvalues()) {
        if (i.has_copysetinfo()) {
            (*key2Info)[GetCopysetKey(i.copysetinfo().copysetid(),
                                      i.copysetinfo().poolid())]
                .push_back(i);
        } else {
            ret = false;
        }
    }
    return ret;
}

bool Response2CopysetInfo(
    const curvefs::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t, std::vector<curvefs::mds::topology::CopysetValue>>*
        key2Info) {
    bool ret = true;
    for (auto const& i : response.copysetvalues()) {
        if (i.has_copysetinfo()) {
            (*key2Info)[GetCopysetKey(i.copysetinfo().copysetid(),
                                      i.copysetinfo().poolid())]
                .push_back(i);
        } else {
            ret = false;
        }
    }
    return ret;
}

CheckResult checkCopysetHelthy(
    const std::vector<curvefs::mds::topology::CopysetValue>& copysetInfoVec,
    const std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>&
        copysetStatusVec) {
    if (copysetInfoVec.empty()) {
        return CheckResult::kNoCopyset;
    }
    if (copysetInfoVec.size() > 1) {
        return CheckResult::kOverCopyset;
    }
    if (curvefs::mds::topology::TopoStatusCode::TOPO_OK !=
        copysetInfoVec[0].statuscode()) {
        return CheckResult::kTopoNotOk;
    }
    auto copysetInfo = copysetInfoVec[0].copysetinfo();
    if (static_cast<size_t>(copysetInfo.peers().size()) !=
        copysetStatusVec.size()) {
        return CheckResult::kPeersNoSufficient;
    }

    return CheckResult::kHealthy;
}

}  // namespace copyset
}  // namespace tools
}  // namespace curvefs
