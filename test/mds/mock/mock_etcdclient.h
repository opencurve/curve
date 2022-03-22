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
 * Created Date: 20190709
 * Author: lixiaocui
 */

#ifndef  TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_
#define  TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <utility>
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/lru_cache.h"

namespace curve {
namespace mds {

using ::curve::kvstorage::EtcdClientImp;
using Cache =
    ::curve::common::LRUCacheInterface<std::string, std::string>;

class MockEtcdClient : public EtcdClientImp {
 public:
    virtual ~MockEtcdClient() {}
    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List,
        int(const std::string&, const std::string&, std::vector<std::string>*));
    MOCK_METHOD3(List, int(const std::string&, const std::string&,
                           std::vector<std::pair<std::string, std::string>>*));
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

class MockLRUCache : public Cache {
 public:
    virtual ~MockLRUCache() {}
    MOCK_METHOD2(Put, void(
        const std::string&, const std::string&));
    MOCK_METHOD3(Put, bool(
        const std::string&, const std::string&, std::string*));
    MOCK_METHOD2(Get, bool(const std::string&, std::string*));
    MOCK_METHOD0(Size, uint64_t());
    MOCK_METHOD1(Remove, void(const std::string&));
};
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_MOCK_MOCK_ETCDCLIENT_H_
