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
 * File Created: 2019-12-03
 * Author: charisu
 */

#include "src/tools/etcd_client.h"

#include <memory>

namespace curve {
namespace tool {

int EtcdClient::Init(const std::string &etcdAddr) {
    curve::common::SplitString(etcdAddr, ",", &etcdAddrVec_);
    if (etcdAddrVec_.empty()) {
        std::cout << "Split etcd address fail!" << std::endl;
        return -1;
    }
    return 0;
}

int EtcdClient::GetEtcdClusterStatus(std::vector<std::string> *leaderAddrVec,
                                     std::map<std::string, bool> *onlineState) {
    if (!leaderAddrVec || !onlineState) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    for (const auto &addr : etcdAddrVec_) {
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
        Json::CharReaderBuilder builder;
        Json::CharReaderBuilder::strictMode(&builder.settings_);
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        Json::Value value;
        JSONCPP_STRING errormsg;
        if (!reader->parse(resp.data(), resp.data() + resp.length(), &value,
                           &errormsg)) {
            std::cout << "Parse the response fail! Error: " << errormsg
                      << std::endl;
            return -1;
        }

        if (!value[kEtcdLeader].isNull()) {
            if (value[kEtcdLeader] == value[kEtcdHeader][kEtcdMemberId]) {
                leaderAddrVec->emplace_back(addr);
            }
        }
    }
    return 0;
}

int EtcdClient::GetAndCheckEtcdVersion(std::string *version,
                                       std::vector<std::string> *failedList) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    VersionMapType versionMap;
    for (const auto &addr : etcdAddrVec_) {
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
            std::cout << "Access " << addr + kEtcdVersionUri
                      << " failed, error text: " << cntl.ErrorText()
                      << std::endl;
            failedList->emplace_back(addr);
            continue;
        }
        std::string resp = cntl.response_attachment().to_string();

        Json::CharReaderBuilder builder;
        Json::CharReaderBuilder::strictMode(&builder.settings_);
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        Json::Value value;
        JSONCPP_STRING errormsg;
        if (!reader->parse(resp.data(), resp.data() + resp.length(), &value,
                           &errormsg)) {
            std::cout << "Parse the response fail! Error: " << errormsg
                      << std::endl;
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
