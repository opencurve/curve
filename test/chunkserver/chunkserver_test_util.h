/*
 * Project: curve
 * Created Date: 18-11-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_TEST_UTIL_H
#define CURVE_CHUNKSERVER_TEST_UTIL_H

#include <butil/status.h>

#include <string>

#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;

std::string Exec(const char *cmd);

/**
 * 当前chunkfilepool需要事先格式化，才能使用，此函数用于事先格式化chunkfilepool
 * @param fsptr:本文文件系统指针
 * @param chunkfileSize:chunk文件的大小
 * @param metaPageSize:chunk文件的meta page大小
 * @param poolpath:文件池的路径，例如./chunkfilepool/
 * @param metaPath:meta文件路径，例如./chunkfilepool/chunkfilepool.meta
 * @return 初始化成功返回ChunkfilePool指针，否则返回null
 */
std::shared_ptr<ChunkfilePool> InitChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr,    //NOLINT
                                                 int chunkfileCount,
                                                 int chunkfileSize,
                                                 int metaPageSize,
                                                 std::string poolpath,
                                                 std::string metaPath);

int StartChunkserver(const char *ip,
                     int port,
                     const char *copysetdir,
                     const char *confs,
                     const int snapshotInterval,
                     const int electionTimeoutMs);

butil::Status WaitLeader(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         PeerId *leaderId,
                         int electionTimeoutMs);

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_TEST_UTIL_H
