/*
 * Project: curve
 * Created Date: Mon Nov 19 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
#define CURVE_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_

#include <vector>
#include <map>
#include <atomic>
#include <string>

#include "src/mds/topology/topology_manager.h"
#include "src/mds/common/topology_define.h"
#include "src/mds/schedule/coordinator.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/common/rw_lock.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::TopologyManager;
using ::curve::mds::schedule::Coordinator;
using ::curve::mds::schedule::TopoAdapter;
using ::std::chrono::steady_clock;

namespace curve {
namespace mds {
namespace heartbeat {
struct HeartbeatOption {
  HeartbeatOption() : HeartbeatOption(0, 0, 0) {}
  HeartbeatOption(uint64_t heartbeatInterval,
                  uint64_t heartbeatMissTimeout,
                  uint64_t offLineTimeout) {
      this->heartbeatInterval = heartbeatInterval;
      this->heartbeatMissTimeOut = heartbeatMissTimeout;
      this->offLineTimeOut = offLineTimeout;
  }

  // heartbeatInterval: 正常心跳间隔.
  // chunkServer每隔heartbeatInterval的时间会给mds发送心跳
  uint64_t heartbeatInterval;
  // heartbeatMissTimeout: 心跳超时时间
  // 网络抖动总是存在的，后台线程在检测过程如果发现chunkserver在heartbeatMissTimeOut的
  // 时间内没有心跳, 做报警处理
  uint64_t heartbeatMissTimeOut;
  // offLineTimeOut: 在offlineTimeOut的时间内如果没有收到chunkserver上报的心跳，
  // 把chunkserver的状态置为offline, 并报警。
  // schedule根据chunkserver的状态进行调度。
  uint64_t offLineTimeOut;
};

struct HeartbeatInfo {
  HeartbeatInfo() :
    HeartbeatInfo(0, steady_clock::time_point(), true) {}
  HeartbeatInfo(ChunkServerIdType id,
                const steady_clock::time_point &time,
                bool flag) {
      this->csId = id;
      this->lastReceivedTime = time;
      this->OnlineFlag = flag;
  }
  ChunkServerIdType csId;
  steady_clock::time_point lastReceivedTime;
  bool OnlineFlag;
};

class HeartbeatManager {
 public:
  explicit HeartbeatManager(
      std::shared_ptr<Topology> topology,
      std::shared_ptr<Coordinator> coordinator,
      std::shared_ptr<TopoAdapter> topoAdapter)
      : topology_(topology),
        coordinator_(coordinator),
        topoAdapter_(topoAdapter) {
      isStop_ = true;
  }

  ~HeartbeatManager() {}

  /**
   * @brief 初始化心跳模块
   *
   * @return 错误码
   */
  int Init(const HeartbeatOption &option);

  /**
   * @brief 运行心跳后端线程，执行心跳超时检查, offline检查
   *
   * 使用一定时器，每隔heartbeatMissTimeOut_周期时间执行如下检查：
   * 1. OnlineFlag 初始值为false
   * 2. 当OnlineFlag 值为false时，
   *   若当前时间 - 上次心跳到达时间 <= heartbeatMissTimeOut_,
   *   则置OnlineFlag为true,并更新topology中OnlineState为ONLINE
   * 3. 当OnlineFlag 值为true时，
   *   若当前时间 - 上次心跳到达时间 > heartbeatMissTimeOut_，
   *   则报心跳miss报警
   * 4. 若当前时间 - 上次心跳到达时间 > offLineTimeOut_,
   *   则置OnlineFlag为false, 并更新topology中OnlineState为OFFLINE, 并报警
   */
  void Run();

  /**
   * @brief 停止心跳后端线程
   */
  void Stop();

  /**
   * @brief 更新最近一次心跳到达时间
   */
  void UpdateLastReceivedHeartbeatTime(ChunkServerIdType csId,
                                       const steady_clock::time_point &time);

  /**
   * @brief 处理心跳请求
   */
  void ChunkServerHeartbeat(const ChunkServerHeartbeatRequest &request,
                            ChunkServerHeartbeatResponse *response);

  /**
   * @brief 心跳超时检查后端线程
   */
  void HeartbeatBackEnd();

  void CheckHeartBeatInterval();

  // for test
  bool GetHeartBeatInfo(ChunkServerIdType id, HeartbeatInfo *info);

 private:
  /**
  * @brief 处理copyset信息，调用src/mds/schedule/coordinator.h中CopySetHeartbeat
  * 接口上传copyset信息， 并获取当前配置变更操作, 由leader chunkServer调用
  *
  * @param copysetInfo copyset信息
  * @param copysetConf 出参，heartbeat根据上报的copysetInfo做相应的处理，如果需要做
  * 配置变更，通过copysetConf下发
  *
  * @return 如果有配置变更下发则返回true，没有配置变更下发则返回false
  */
  bool HandleCopysetInfo(const CopysetInfo &copysetInfo,
                         CopysetConf *copysetConf);

  ChunkServerIdType GetPeerIdByIPPort(const std::string &ipPort);

  bool FromHeartbeatCopySetInfoToTopologyOne(
      const ::curve::mds::heartbeat::CopysetInfo &info,
      ::curve::mds::topology::CopySetInfo *out);

  bool PatrolCopySetInfo(const ::curve::mds::topology::CopySetInfo &reportInfo,
                         CopysetConf *conf);

  std::string GetIpPortByChunkServerId(ChunkServerIdType id);

  bool CheckRequest(const ChunkServerHeartbeatRequest &request);

 private:
  std::map<ChunkServerIdType, HeartbeatInfo> heartbeatInfos_;
  std::shared_ptr<Topology> topology_;
  std::shared_ptr<Coordinator> coordinator_;
  std::shared_ptr<TopoAdapter> topoAdapter_;

  // 持有schedule接口对象
  // 心跳时间
  uint64_t heartbeatIntervalSec_;
  // 心跳超时时间
  uint64_t heartbeatMissTimeOutSec_;
  // offline检测超时时间
  uint64_t offLineTimeOutSec_;

  mutable curve::common::RWLock hbinfoLock_;

  // TODO(lixiaocui): 这里这些建议换成统一封装的thread aomic等
  std::thread backEndThread_;
  std::atomic_bool isStop_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
