/*
 * Project: curve
 * Created Date: Mon March 13 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_ETCDCLIENT_H_
#define  TEST_MDS_NAMESERVER2_MOCK_ETCDCLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include "src/mds/nameserver2/etcd_client.h"
#include "src/mds/nameserver2/namespace_storage_cache.h"

namespace curve {
namespace mds {
class MockEtcdClient : public EtcdClientImp {
 public:
    virtual ~MockEtcdClient() {}
    MOCK_METHOD2(Put, int(std::string, std::string));
    MOCK_METHOD2(Get, int(std::string, std::string*));
    MOCK_METHOD3(List,
        int(std::string, std::string, std::vector<std::string>*));
    MOCK_METHOD1(Delete, int(std::string));
    MOCK_METHOD2(Txn2, int(Operation, Operation));
    MOCK_METHOD3(CompareAndSwap, int(std::string, std::string, std::string));
};

class MockLRUCache : public LRUCache {
 public:
    virtual ~MockLRUCache() {}
    MOCK_METHOD2(Put, void(const std::string&, const std::string&));
    MOCK_METHOD2(Get, bool(const std::string&, std::string*));
    MOCK_METHOD1(Remove, void(const std::string&));
};
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_NAMESERVER2_MOCK_ETCDCLIENT_H_
