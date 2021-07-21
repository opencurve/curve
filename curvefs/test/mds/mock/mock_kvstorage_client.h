
/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Jul 30 17:28:54 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_KVSTORAGE_CLIENT_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_KVSTORAGE_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {

class MockKVStorageClient : public curve::kvstorage::KVStorageClient {
 public:
    MockKVStorageClient() = default;
    ~MockKVStorageClient() = default;

    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List, int(const std::string&, const std::string&,
                           std::vector<std::string>*));
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
    MOCK_METHOD1(GetCurrentRevision, int(int64_t*));
    MOCK_METHOD6(ListWithLimitAndRevision,
                 int(const std::string&, const std::string&, int64_t, int64_t,
                     std::vector<std::string>*, std::string*));
    MOCK_METHOD3(PutRewithRevision,
                 int(const std::string&, const std::string&, int64_t*));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string&, int64_t*));
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_KVSTORAGE_CLIENT_H_
