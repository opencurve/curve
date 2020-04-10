/*
 * Project: curve
 * File Created: 2019-12-03
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include "src/tools/etcd_client.h"

namespace curve {
namespace tool {

int EtcdClient::Init(const std::string& etcdAddr) {
    curve::common::SplitString(etcdAddr, ",", &etcdAddrVec_);
    if (etcdAddrVec_.empty()) {
        std::cout << "Split etcd address fail!" << std::endl;
        return -1;
    }
    return 0;
}

int EtcdClient::GetEtcdClusterStatus(std::string* leaderAddr,
                        std::map<std::string, bool>* onlineState) {
    if (!leaderAddr || !onlineState) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    *leaderAddr = "";
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    for (const auto& addr : etcdAddrVec_) {
        int res = httpChannel.Init(addr.c_str(), &options);
        if (res != 0) {
            (*onlineState)[addr] = false;
            continue;
        }
        brpc::Controller cntl;
        cntl.http_request().uri() = addr + kEtcdStatusUri;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        if (cntl.Failed()) {
            (*onlineState)[addr] = false;
            continue;
        }
        (*onlineState)[addr] = true;
        std::string resp = cntl.response_attachment().to_string();
        Json::Reader reader(Json::Features::strictMode());
        Json::Value value;
        if (!reader.parse(resp, value)) {
            std::cout << "Parse the response fail!" << std::endl;
            return -1;
        }
        if (!value[kEtcdLeader].isNull()) {
            if (value[kEtcdLeader] == value[kEtcdHeader][kEtcdMemberId]) {
                *leaderAddr = addr;
            }
        }
    }
    return 0;
}

int EtcdClient::GetAndCheckEtcdVersion(std::string* version,
                                       std::vector<std::string>* failedList) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    VersionMapType versionMap;
    for (const auto& addr : etcdAddrVec_) {
        int res = httpChannel.Init(addr.c_str(), &options);
        if (res != 0) {
            std::cout << "Init channel to " << addr << " failed" << std::endl;
            failedList->emplace_back(addr);
            continue;
        }
        brpc::Controller cntl;
        cntl.http_request().uri() = addr + kEtcdVersionUri;
        httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        if (cntl.Failed()) {
            std::cout << "Access " << addr + kEtcdVersionUri << " failed"
                      << std::endl;;
            failedList->emplace_back(addr);
            continue;
        }
        std::string resp = cntl.response_attachment().to_string();
        Json::Reader reader(Json::Features::strictMode());
        Json::Value value;
        if (!reader.parse(resp, value)) {
            std::cout << "Parse the response fail!" << std::endl;
            return -1;
        }
        if (value[kEtcdCluster].isNull()) {
            std::cout << "Parse cluster version from response failed"
                      << std::endl;
            failedList->emplace_back(addr);
            continue;
        }
        std::string ver = value[kEtcdCluster].asString();
        if (versionMap.find(ver) == versionMap.end()) {
            versionMap[ver] = {addr};
        } else {
            versionMap[ver].emplace_back(addr);
        }
    }
    if (versionMap.empty()) {
        std::cout << "no version found!" << std::endl;
        return -1;
    } else if (versionMap.size() > 1) {
        std::cout << " version not match, version map: ";
        VersionTool::PrintVersionMap(versionMap);
        return -1;
    } else {
        *version = versionMap.begin()->first;
    }
    return 0;
}

}  // namespace tool
}  // namespace curve
