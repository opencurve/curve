/*
 * Project: curve
 * Created Date: Monday September 10th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_

#include <string>
#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/mds/kvstorageclient/etcd_client.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Atomic;

namespace curve {
namespace mds {
class EtcdIdGenerator {
 public:
    EtcdIdGenerator(
        const std::shared_ptr<KVStorageClient> &client,
        const std::string &storeKey, uint64_t initial, uint64_t bundle) :
        client_(client), storeKey_(storeKey), initialize_(initial),
        bundle_(bundle), nextId_(initial), bundleEnd_(initial) {}
    virtual ~EtcdIdGenerator() {}


    bool GenID(InodeID *id);

 private:
    /*
    * @brief 从storage中批量申请ID
    *
    * @param[in] requiredNum 需要申请的id个数
    *
    * @param[out] false表示申请失败，true表示申请成功
    */
    bool AllocateBundleIds(int requiredNum);

 private:
    std::string storeKey_;
    uint64_t initialize_;
    uint64_t bundle_;

    std::shared_ptr<KVStorageClient> client_;
    uint64_t nextId_;
    uint64_t bundleEnd_;

    ::curve::common::RWLock lock_;
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_
