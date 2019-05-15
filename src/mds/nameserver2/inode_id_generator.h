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
#include "src/mds/nameserver2/etcd_id_generator.h"
#include "src/mds/nameserver2/namespace_helper.h"

namespace curve {
namespace mds {
const uint64_t INODEBUNDLEALLOCATED = 1000;

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
    explicit InodeIdGeneratorImp(std::shared_ptr<StorageClient> client) {
        generator_ = std::make_shared<EtcdIdGenerator>(
            client, INODESTOREKEY, USERSTARTINODEID, INODEBUNDLEALLOCATED);
    }
    virtual ~InodeIdGeneratorImp() {}

    bool GenInodeID(InodeID *id) override;

 private:
    std::shared_ptr<EtcdIdGenerator> generator_;
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_INODE_ID_GENERATOR_H_
