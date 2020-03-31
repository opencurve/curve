/*
 * Project: curve
 * File Created: Tuesday, 7th May 2019 11:32:51 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_COMMON_NET_COMMON_H_
#define SRC_COMMON_NET_COMMON_H_

#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>    // in_addr
#include <arpa/inet.h>     // inet_pton, inet_ntop
#include <glog/logging.h>
#include <string>

namespace curve {
namespace common {
class NetCommon {
 public:
    // addr形式为"ip:port"
    static bool CheckAddressValid(const std::string& addr) {
        std::string ip;
        uint32_t port;
        return SplitAddrToIpPort(addr, &ip, &port);
    }

    // addr形式为"ip:port"
    static bool SplitAddrToIpPort(const std::string& addr,
                                  std::string* ipstr,
                                  uint32_t* port) {
        size_t splitpos = addr.find(":");
        if (splitpos == -1) {
            LOG(ERROR) << "address invalid!";
            return false;
        }

        *ipstr = addr.substr(0, splitpos);
        *port = atol(addr.substr(splitpos + 1, addr.npos).c_str());

        in_addr ip1;
        int rc = inet_pton(AF_INET, ipstr->c_str(), static_cast<void*>(&ip1));
        if (rc <= 0) {
            LOG(ERROR) << "ip string invlid: " << ipstr->c_str();
            return false;
        }

        if (*port <= 0 || *port >= 65535) {
            LOG(ERROR) << "Invalid port provided: " << port;
            return false;
        }
        return true;
    }

    static bool GetLocalIP(std::string* ipstr) {
        char hostname[128];
        int ret = gethostname(hostname, sizeof(hostname));

        if (ret == -1) {
            LOG(INFO) << "get hostname failed!";
            return false;
        }

        struct hostent* t;
        t = gethostbyname(hostname);

        if (t == nullptr) {
            LOG(INFO) << "get host ip failed!";
            return false;
        }

        char* ip = inet_ntoa(*(struct in_addr*)t->h_addr_list[0]);
        *ipstr = std::string(ip);

        return true;
    }
};
}   // namespace common
}   // namespace curve

#endif  // SRC_COMMON_NET_COMMON_H_
