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
        cntl.http_request().uri() = addr + statusUri_;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        if (cntl.Failed()) {
            (*onlineState)[addr] = false;
            continue;
        }
        (*onlineState)[addr] = true;
        std::string resp = cntl.response_attachment().to_string();
        Json::Reader reader;
        Json::Value value;
        if (!reader.parse(resp, value)) {
            std::cout << "Parse the response fail!" << std::endl;
            return -1;
        }
        if (!value["leader"].isNull()) {
            if (value["leader"] == value["header"]["member_id"]) {
                *leaderAddr = addr;
            }
        }
    }
    return 0;
}
}  // namespace tool
}  // namespace curve
