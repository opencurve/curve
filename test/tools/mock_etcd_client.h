/*
 * Project: curve
 * File Created: 2019-12-05
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_ETCD_CLIENT_H_
#define TEST_TOOLS_MOCK_ETCD_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <map>
#include "src/tools/etcd_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockEtcdClient : public EtcdClient {
 public:
    MockEtcdClient() {}
    ~MockEtcdClient() {}
    MOCK_METHOD1(Init, int(const std::string &));
    MOCK_METHOD2(GetEtcdClusterStatus, int(std::string*,
                                    std::map<std::string, bool>*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_ETCD_CLIENT_H_
