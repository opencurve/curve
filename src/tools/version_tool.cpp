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
 * Created Date: 2020-02-18
 * Author: charisu
 */

#include "src/tools/version_tool.h"

namespace curve {
namespace tool {

int VersionTool::GetAndCheckMdsVersion(std::string* version,
                                       std::vector<std::string>* failedList) {
    const auto& dummyServerMap = mdsClient_->GetDummyServerMap();
    std::vector<std::string> dummyServers;
    for (const auto& item : dummyServerMap) {
        dummyServers.emplace_back(item.second);
    }
    VersionMapType versionMap;
    GetVersionMap(dummyServers, &versionMap, failedList);
    int ret = 0;
    if (versionMap.empty()) {
        std::cout << "no version found!" << std::endl;
        ret = -1;
    } else if (versionMap.size() > 1) {
        std::cout << "mds version not match, version map: ";
        PrintVersionMap(versionMap);
        ret = -1;
    } else {
        *version = versionMap.begin()->first;
    }
    return ret;
}

int VersionTool::GetAndCheckChunkServerVersion(std::string* version,
                                        std::vector<std::string>* failedList) {
    std::vector<ChunkServerInfo> chunkServers;
    int res = mdsClient_->ListChunkServersInCluster(&chunkServers);
    if (res != 0) {
        std::cout << "ListChunkServersInCluster fail!" << std::endl;
        return -1;
    }
    std::vector<std::string> csAddrs;
    for (const auto& cs : chunkServers) {
        std::string addr = cs.hostip() + ":" + std::to_string(cs.port());
        csAddrs.emplace_back(addr);
    }

    VersionMapType versionMap;
    GetVersionMap(csAddrs, &versionMap, failedList);
    int ret = 0;
    if (versionMap.empty()) {
        std::cout << "no version found!" << std::endl;
        ret = -1;
    } else if (versionMap.size() > 1) {
        std::cout << "chunkserver version not match, version map: ";
        PrintVersionMap(versionMap);
        ret = -1;
    } else {
        *version = versionMap.begin()->first;
    }
    return ret;
}

int VersionTool::GetAndCheckSnapshotCloneVersion(std::string* version,
                                        std::vector<std::string>* failedList) {
    const auto& dummyServerMap = snapshotClient_->GetDummyServerMap();
    std::vector<std::string> dummyServers;
    for (const auto& item : dummyServerMap) {
        dummyServers.emplace_back(item.second);
    }
    VersionMapType versionMap;
    GetVersionMap(dummyServers, &versionMap, failedList);
    int ret = 0;
    if (versionMap.empty()) {
        std::cout << "no version found!" << std::endl;
        ret = -1;
    } else if (versionMap.size() > 1) {
        std::cout << "snapshot clone version not match, version map: ";
        PrintVersionMap(versionMap);
        ret = -1;
    } else {
        *version = versionMap.begin()->first;
    }
    return ret;
}

int VersionTool::GetClientVersion(ClientVersionMapType* versionMap) {
    std::vector<std::string> clientAddrs;
    int res = mdsClient_->ListClient(&clientAddrs);
    if (res != 0) {
        std::cout << "ListClient fail" << std::endl;
        return -1;
    }
    ProcessMapType processMap;
    FetchClientProcessMap(clientAddrs, &processMap);
    for (const auto& item : processMap) {
        VersionMapType map;
        std::vector<std::string> failedList;
        GetVersionMap(item.second, &map, &failedList);
        (*versionMap)[item.first] = map;
    }
    return 0;
}

void VersionTool::FetchClientProcessMap(const std::vector<std::string>& addrVec,
                                        ProcessMapType* processMap) {
    for (const auto& addr : addrVec) {
        std::string cmd;
        MetricRet res = metricClient_->GetMetric(addr,
                                            kProcessCmdLineMetricName,
                                            &cmd);
        if (res != MetricRet::kOK) {
            continue;
        }
        std::string processName = GetProcessNameFromCmd(cmd);
        if (processMap->find(processName) == processMap->end()) {
            (*processMap)[processName] = {addr};
        } else {
            (*processMap)[processName].emplace_back(addr);
        }
    }
}

std::string VersionTool::GetProcessNameFromCmd(const std::string& cmd) {
    if (cmd.find(kProcessNebdServer) != cmd.npos) {
        return kProcessNebdServer;
    } else if (cmd.find(kProcessPython) != cmd.npos) {
        return kProcessPython;
    } else if (cmd.find(kProcessQemu) != cmd.npos) {
        return kProcessQemu;
    } else {
        return kProcessOther;
    }
}

void VersionTool::GetVersionMap(const std::vector<std::string>& addrVec,
                                VersionMapType* versionMap,
                                std::vector<std::string>* failedList) {
    failedList->clear();
    for (const auto& addr : addrVec) {
        std::string version;
        MetricRet res = metricClient_->GetMetric(addr, kCurveVersionMetricName,
                                           &version);
        if (res != MetricRet::kOK) {
            // 0.0.5.2版本之前没有curve_version的metric，因此再判断一下
            if (res == MetricRet::kNotFound) {
                version = kOldVersion;
            } else {
                failedList->emplace_back(addr);
                continue;
            }
        }
        if (versionMap->find(version) == versionMap->end()) {
            (*versionMap)[version] = {addr};
        } else {
            (*versionMap)[version].emplace_back(addr);
        }
    }
}

void VersionTool::PrintVersionMap(const VersionMapType& versionMap) {
    for (const auto& item : versionMap) {
        std::cout << item.first << ": {";
        for (uint32_t i = 0; i < item.second.size(); ++i) {
            if (i != 0) {
                std::cout << ", ";
            }
            std::cout << item.second[i];
        }
        std::cout << "}" << std::endl;
    }
}

void VersionTool::PrintFailedList(const std::vector<std::string>& failedList) {
    std::cout << "get version from {";
    for (uint32_t i = 0; i < failedList.size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << failedList[i];
    }
    std::cout << "} fail" << std::endl;
}

}  // namespace tool
}  // namespace curve
