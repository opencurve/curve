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
 * @Project: curve
 * @Date: 2021-08-31
 * @Author: chengyi
 */

#ifndef CURVEFS_TEST_MDS_MOCK_ETCDCLIENT_H_
#define CURVEFS_TEST_MDS_MOCK_ETCDCLIENT_H_

#include <gmock/gmock.h>

#include <string>
#include <utility>
#include <vector>

#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {

using curve::kvstorage::EtcdClientImp;

class MockEtcdClientImpl : public KVStorageClient {
 public:
    virtual ~MockEtcdClientImpl() {
        // LOG(INFO) << "test";
    }
    MOCK_METHOD0(CloseClient, void());
    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD3(PutRewithRevision,
                 int(const std::string&, const std::string&, int64_t*));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List, int(const std::string&, const std::string&,
                           std::vector<std::string>*));
    MOCK_METHOD3(List, int(const std::string&, const std::string&,
                           std::vector<std::pair<std::string, std::string>>*));
    MOCK_METHOD1(Delete, int(const std::string&));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string&, int64_t*));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation>&));
    MOCK_METHOD3(CompareAndSwap, int(const std::string&, const std::string&,
                                     const std::string&));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t*));
    MOCK_METHOD6(ListWithLimitAndRevision,
                 int(const std::string&, const std::string&, int64_t, int64_t,
                     std::vector<std::string>*, std::string*));
    // MOCK_METHOD5(CampaignLeader, int(const std::string&, const
    // std::string&,
    //                                  uint32_t, uint32_t, uint64_t*))
    // MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string&));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(SetTimeout, void(int));
    MOCK_METHOD1(NeedRetry, bool(int));
};

}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_MOCK_ETCDCLIENT_H_
