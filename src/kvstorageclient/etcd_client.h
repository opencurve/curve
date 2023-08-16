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
 * Created Date: Saturday March 9th 2019
 * Author: lixiaocui1
 */

#ifndef SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_
#define SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_

//#include <libetcdclient.h>
#include <string>
#include <vector>
#include <utility>
#include "include/etcdclient/storageclient.h"

namespace curve {
namespace kvstorage {

// encapsulate the c header file of etcd generated by go compilation
class EtcdClientImp : public StorageClient {
 public:
    EtcdClientImp() {}
    ~EtcdClientImp() {
        CloseClient();
    }

    /**
     * @brief Init init the etcdclient, a global var in go
     *
     * @param[in] conf the configuration for init etcdclient
     * @param[in] timeout
     * @param[in] retryTimes
     *
     * @return return error code EtcdErrCode
     */
    int Init(EtcdConf conf, int timeout, int retryTiems);

    void CloseClient();

    int Put(const std::string &key, const std::string &value) override;

    int PutRewithRevision(const std::string &key, const std::string &value,
        int64_t *revision) override;

    int Get(const std::string &key, std::string *out) override;

    int List(const std::string &startKey,
        const std::string &endKey, std::vector<std::string> *values) override;

    int List(const std::string& startKey, const std::string& endKey,
             std::vector<std::pair<std::string, std::string> >* out) override;

    int Delete(const std::string &key) override;

    int DeleteRewithRevision(
        const std::string &key, int64_t *revision) override;

    int TxnN(const std::vector<Operation> &ops) override;

    int CompareAndSwap(const std::string &key, const std::string &preV,
        const std::string &target) override;

    virtual int GetCurrentRevision(int64_t *revision);

    /**
     * @brief ListWithLimitAndRevision
     *        get key-value pairs between [startKey, endKey)
     *        with specify number and revision
     *
     * @param[in] startKey start key
     * @param[in] endKey end key, not included
     * @param[in] limit max number
     * @param[in] revision get the key <= revision
     * @param[out] values the value vector of all the key-value pairs
     * @param[out] lastKey the last key of the vector
     */
    virtual int ListWithLimitAndRevision(const std::string &startKey,
        const std::string &endKey, int64_t limit, int64_t revision,
        std::vector<std::string> *values, std::string *lastKey);

    /**
     * @brief CampaignLeader Leader campaign through etcd, return directly if
     *                       the election is successful. Otherwise, if
     *                       electionTimeoutMs>0, it will return failure; if
     *                       electionTimeoutMs=0, it will block until become
     *                       leader or fatal error appears.
     *
     * @param[in] pfx the key for leader campaign
     * @param[in] leaderName leader name, the form ip+port is suggested
     * @param[in] sessionInterSec used to create session with ttl, the etcd
     *                            will delete the peers registered by this
     *                            leader when the session expired after
     *                            client offline.
     * @param[in] electionTimeoutMs the timeout，0 will block always
     * @param[out] leaderOid leader的objectId，recorded in objectManager
     *
     * @return EtcdErrCode::EtcdCampaignLeaderSuccess success，others fail
     */
    virtual int CampaignLeader(
        const std::string &pfx, const std::string &leaderName,
        uint32_t sessionInterSec, uint32_t electionTimeoutMs,
        uint64_t *leaderOid);

    /**
     * @brief LeaderObserve
     *        observe the session between mds and etcd,
     *        return error if the session closed or expired
     *
     * @param[in] leaderOid observe the leader
     * @param[in] leaderName the name for leader campaign
     *
     * @return if returned, the session between mds and etcd expired
     */
    virtual int LeaderObserve(
        uint64_t leaderOid, const std::string &leaderName);

    /**
     * @brief LeaderResign the leader resigns initiatively, the other peers
     *                     could campaign for leader if successed
     *
     * @param[in] leaderOid the specify leader
     * @param[in] timeoutMs the timeout of get/txn operations of client
     *
     * @return EtcdErrCode::EtcdLeaderResiginSuccess resign seccess
     *         EtcdErrCode::EtcdLeaderResiginErr resign fail
     */
     virtual int LeaderResign(uint64_t leaderOid, uint64_t timeoutMs);

    // for test
    void SetTimeout(int time);

 private:
    bool NeedRetry(int errCode);

 private:
    // the interface timeout，unit is milliseconds
    int timeout_;
    // retry times when failed
    int retryTimes_;
};
}  // namespace kvstorage
}  // namespace curve

#endif  // SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_
