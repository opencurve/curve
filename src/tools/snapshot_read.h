/*
 * Project: curve
 * Created Date: 2019-10-22
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#ifndef SRC_TOOLS_SNAPSHOT_READ_H_
#define SRC_TOOLS_SNAPSHOT_READ_H_

#include <string.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <map>

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"
#include "proto/snapshotcloneserver.pb.h"

using curve::common::Configuration;
using curve::snapshotcloneserver::SnapshotCloneRepo;
using curve::snapshotcloneserver::SnapshotRepoItem;
using curve::snapshotcloneserver::ChunkMap;

namespace curve {
namespace tool {
class SnapshotRead {
 public:
    SnapshotRead(std::shared_ptr<SnapshotCloneRepo> repo,
                  std::shared_ptr<curve::common::S3Adapter> s3Adapter) :
                        repo_(repo), s3Adapter_(s3Adapter) {}
    SnapshotRead() {}
    virtual ~SnapshotRead() = default;

    /**
     * 初始化
     */
    virtual int Init(const std::string& snapshotId,
                     const std::string& confPath);

    /**
     * 释放资源
     */
    virtual void UnInit();

    /**
     *  @brief 读取快照内容
     *  @param buf：当前待读取的缓冲区
     *  @param offset：文件内的偏移
     *  @param len：待读取的长度
     *  @return 成功返回0，失败返回-1
     */
    virtual int Read(char* buf, off_t offset, size_t len);

    virtual void GetSnapshotInfo(SnapshotRepoItem *item);

 private:
    int InitChunkMap();

    std::shared_ptr<SnapshotCloneRepo> repo_;
    std::shared_ptr<curve::common::S3Adapter> s3Adapter_;
    // snapshot的一些信息
    SnapshotRepoItem item_;
    // 存储chunkIndex到seqNum的映射
    std::map<uint64_t, std::string> chunkMap_;
};

}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_SNAPSHOT_READ_H_
