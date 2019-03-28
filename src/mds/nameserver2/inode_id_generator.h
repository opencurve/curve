/*
 * Project: curve
 * Created Date: Monday September 10th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_INODE_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_INODE_ID_GENERATOR_H_

#include <string>

#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/etcd_client.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Atomic;

namespace curve {
namespace mds {
const uint64_t INODEINITIALIZE = 0;
const uint64_t BUNDLEALLOCATED = 1000;

class InodeIDGenerator {
 public:
    virtual ~InodeIDGenerator() {}

    /*
    * @brief GenInodeId 生成全局递增的id
    *
    * @param[out] 生成的id
    *
    * @return true表示生成成功， false表示生成失败
    */
    virtual bool GenInodeID(InodeID * id) = 0;
};

class InodeIdGeneratorImp : public InodeIDGenerator {
 public:
    explicit InodeIdGeneratorImp(std::shared_ptr<StorageClient> client) :
        client_(client),
        nextId_(INODEINITIALIZE),
        bundleEnd_(INODEINITIALIZE) {}
    virtual ~InodeIdGeneratorImp() {}

    /*
    * @brief Init 从etcd中申请n个inodeId
    *
    * @return true表示load成功，false表示不成功
    */
    bool Init();

    bool GenInodeID(InodeID *id) override;

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
    std::shared_ptr<StorageClient> client_;
    uint64_t nextId_;
    uint64_t bundleEnd_;

    ::curve::common::RWLock lock_;
};
}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_INODE_ID_GENERATOR_H_
