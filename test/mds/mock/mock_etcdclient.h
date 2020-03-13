/*
 * Project: curve
 * Created Date: 20190709
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_
#define  TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include "src/mds/kvstorageclient/etcd_client.h"
#include "src/mds/nameserver2/namespace_storage_cache.h"

namespace curve {
namespace mds {
class MockEtcdClient : public EtcdClientImp {
 public:
    virtual ~MockEtcdClient() {}
    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List,
        int(const std::string&, const std::string&, std::vector<std::string>*));
    MOCK_METHOD1(Delete, int(const std::string&));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation>&));
    MOCK_METHOD3(CompareAndSwap, int(const std::string&, const std::string&,
        const std::string&));
    MOCK_METHOD5(CampaignLeader, int(const std::string&, const std::string&,
        uint32_t, uint32_t, uint64_t*));
    MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string&));
    MOCK_METHOD2(LeaderKeyExist, bool(uint64_t, uint64_t));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t *));
    MOCK_METHOD6(ListWithLimitAndRevision,
        int(const std::string&, const std::string&,
        int64_t, int64_t, std::vector<std::string>*, std::string *));
    MOCK_METHOD3(PutRewithRevision, int(const std::string &,
        const std::string &, int64_t *));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string &, int64_t *));
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

#endif  // TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_
