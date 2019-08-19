/*
 * Project: curve
 * Created Date: Saturday October 13th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_

#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/nameserver2/idgenerator/etcd_id_generator.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

using ::curve::common::Atomic;

namespace curve {
namespace  mds {
const uint64_t CHUNKINITIALIZE = 0;
const uint64_t CHUNKBUNDLEALLOCATED = 1000;

class ChunkIDGenerator {
 public:
    virtual  ~ChunkIDGenerator() {}

    /*
    * @brief GenChunkID 生成全局递增的id
    *
    * @param[out] 生成的id
    *
    * @return true表示生成成功， false表示生成失败
    */
    virtual bool GenChunkID(ChunkID *id) = 0;
};

class ChunkIDGeneratorImp : public ChunkIDGenerator {
 public:
    explicit ChunkIDGeneratorImp(std::shared_ptr<KVStorageClient> client) {
        generator_ = std::make_shared<EtcdIdGenerator>(
            client, CHUNKSTOREKEY, CHUNKINITIALIZE, CHUNKBUNDLEALLOCATED);
    }
    virtual ~ChunkIDGeneratorImp() {}

    bool GenChunkID(ChunkID *id) override;

 private:
    std::shared_ptr<EtcdIdGenerator> generator_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_
