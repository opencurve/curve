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
 * Created Date: 2021-09-23
 * Author: wanghai01
 */
#ifndef CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_

#include <string>
#include <vector>

#include "src/common/string_util.h"

namespace curvefs {
namespace mds {
namespace topology {

using curve::common::SplitString;
using curve::common::StringToUl;

/**
 * @brief the peerid format is ip:port:index, and be used in braft.
 */
inline std::string BuildPeerIdWithIpPort(const std::string& ip, uint32_t port,
                                         uint32_t idx = 0) {
    return ip + ":" + std::to_string(port) + ":" + std::to_string(idx);
}

inline std::string BuildPeerIdWithAddr(const std::string& addr,
                                       uint32_t idx = 0) {
    return addr + ":" + std::to_string(idx);
}

inline bool SplitPeerId(const std::string& peerId, std::string* ip,
                        uint32_t* port, uint32_t* idx = nullptr) {
    std::vector<std::string> items;
    SplitString(peerId, ":", &items);
    if (3 == items.size()) {
        *ip = items[0];
        if (!StringToUl(items[1], port)) {
            return false;
        }
        if (idx != nullptr) {
            if (!StringToUl(items[2], idx)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

inline bool SplitPeerId(const std::string& peerId, std::string* addr) {
    std::vector<std::string> items;
    curve::common::SplitString(peerId, ":", &items);
    if (3 == items.size()) {
        *addr = items[0] + ":" + items[1];
        return true;
    }
    return false;
}

inline bool SplitAddrToIpPort(const std::string& addr, std::string* ipstr,
                              uint32_t* port) {
    std::vector<std::string> items;
    curve::common::SplitString(addr, ":", &items);
    if (2 == items.size() && curve::common::StringToUl(items[1], port)) {
        *ipstr = items[0];
        return true;
    }
    return false;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_
