/*
 * Project: curve
 * Created Date: 2020-02-18
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include "src/tools/version_tool.h"

namespace curve {
namespace tool {

int VersionTool::Init(const std::string& mdsAddr) {
    return mdsClient_->Init(mdsAddr);
}

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

int VersionTool::GetClientVersion(VersionMapType* versionMap,
                                  std::vector<std::string>* failedList) {
    std::vector<std::string> clientAddrs;
    int res = mdsClient_->ListClient(&clientAddrs);
    if (res != 0) {
        std::cout << "ListClient fail" << std::endl;
        return -1;
    }
    GetVersionMap(clientAddrs, versionMap, failedList);
    return 0;
}

void VersionTool::GetVersionMap(const std::vector<std::string>& addrVec,
                                VersionMapType* versionMap,
                                std::vector<std::string>* failedList) {
    failedList->clear();
    for (const auto& addr : addrVec) {
        std::string version;
        int res = metricClient_->GetMetric(addr, kCurveVersionMetricName,
                                           &version);
        if (res != 0) {
            failedList->emplace_back(addr);
            continue;
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
