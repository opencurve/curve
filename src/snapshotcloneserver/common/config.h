/*************************************************************************
 > File Name: config.h
 > Author:
 > Created Time: Wed Nov 21 11:33:46 2018
    > Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_


#include<string>
#include <vector>

namespace curve {
namespace snapshotcloneserver {
  // curve client options
struct CurveClientOptions {
  // opt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:6666");
  std::string mdsAddr;
  // opt.ioOpt.reqSchdulerOpt.queueCapacity = 4096;
  uint32_t requestQueueCap;
  // opt.ioOpt.reqSchdulerOpt.threadpoolSize = 2;
  uint32_t threadNum;
  // opt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = 3;
  uint32_t requestMaxRetry;
  // opt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
  uint32_t requestRetryIntervalUs;
  // opt.ioOpt.metaCacheOpt.getLeaderRetry = 3;
  uint32_t getLeaderRetry;
  // opt.ioOpt.ioSenderOpt.enableAppliedIndexRead = 1;
  uint32_t enableApplyIndexRead;
  // opt.ioOpt.ioSplitOpt.ioSplitMaxSize = 64;
  uint32_t ioSplitSize;
  // opt.loginfo.loglevel = 0;
  uint32_t loglevel;
};

  // snapshotcloneserver options
struct SnapshotCloneServerOptions {
  // snapshot&clone server address
  std::string  addr;
};

  // metastore options
struct SnapshotCloneMetaStoreOptions {
  // 数据库名称
  std::string dbName;
  // 数据库用户名
  std::string dbUser;
  // 数据库验证密码
  std::string dbPassword;
  // 数据库服务地址
  std::string dbAddr;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
