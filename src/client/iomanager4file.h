/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:15:27 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_IOCONTEXT_MANAGER_H
#define CURVE_IOCONTEXT_MANAGER_H

#include <atomic>
#include <mutex>  // NOLINT
#include <condition_variable>   // NOLINT

#include "src/client/metacache.h"
#include "src/client/iomanager.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"
#include "src/client/request_scheduler.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {
class IOManager4File : public IOManager {
 public:
  IOManager4File();
  ~IOManager4File() = default;

  /**
   * 初始化函数
   * @param: ioopt为当前iomanager的配置信息
   * @return: 成功true,失败false
   */
  bool Initialize(IOOption_t ioOpt);

  /**
   * 同步模式读
   * @param: buf为当前待读取的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @return: 成功返回读取真实长度，-1为失败
   */
  LIBCURVE_ERROR  Read(char* buf,
                      off_t offset,
                      size_t length,
                      MDSClient* mdsclient);
  /**
   * 同步模式写
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: buf为当前待写入的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return： 成功返回写入真实长度，-1为失败
   */
  LIBCURVE_ERROR  Write(const char* buf,
                      off_t offset,
                      size_t length,
                      MDSClient* mdsclient);
  /**
   * 异步模式读
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return： 0为成功，-1为失败
   */
  void AioRead(CurveAioContext* aioctx,
                      MDSClient* mdsclient);
  /**
   * 异步模式写
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return： 0为成功，-1为失败
   */
  void AioWrite(CurveAioContext* aioctx,
                      MDSClient* mdsclient);

  /**
   * 析构，回收资源
   */
  void UnInitialize();

  /**
   * 获取metacache，测试代码使用
   */
  MetaCache* GetMetaCache() {return &mc_;}
  /**
   * 设置scahuler，测试代码使用
   */
  void SetRequestScheduler(RequestScheduler* scheduler) {
    scheduler_ = scheduler;
  }

 private:
  friend class LeaseExcutor;
  /**
   * lease相关接口，当leaseexcutor续约失败的时候，调用LeaseTimeoutDisableIO
   * 将新下发的IO全部失败返回
   */
  void LeaseTimeoutDisableIO();

  /**
   * 当lease又续约成功的时候，leaseexcutor调用该接口恢复IO
   */
  void RefeshSuccAndResumeIO();

  /**
   * 当lesaeexcutor发现版本变更，调用该接口开始等待inflight回来，这段期间IO是hang的
   */
  void StartWaitInflightIO();

  /**
   * lease excutor在检查到版本更新的时候，需要通知iomanager更新文件版本信息
   * @param: fi为当前需要更新的文件信息
   */
  void UpdataFileInfo(const FInfo_t& fi);

  /**
   * 因为curve client底层都是异步IO，每个IO会分配一个IOtracker跟踪IO
   * 当这个IO做完之后，底层需要告知当前io manager来释放这个IOTracker，
   * HandleAsyncIOResponse负责释放IOTracker
   * @param: iotracker是返回的异步io
   */
  void HandleAsyncIOResponse(IOTracker* iotracker) override;

  /**
   * 更新inflight计数
   */
  void RefreshInflightIONum();
  /**
   * 调用该接口等待inflight回来，这段期间IO是hang的
   */
  void WaitInflightIOComeBack();

 private:
  // 当前文件的信息
  FInfo_t fi_;

  // 此锁与inflightcv_条件变量配合使用，在版本变更的时候来hang IO
  std::mutex  mtx_;

  // 每个IOManager都有其IO配置，保存在iooption里
  IOOption_t  ioopt_;

  // lease续约失败时候，置位disableio_
  std::atomic<bool> disableio_;

  // 版本变更的时候置位waitinflightio_
  std::atomic<bool> startWaitInflightIO_;

  // inflightio_记录当前inflight的IO数量
  std::atomic<uint64_t> inflightIONum_;

  // 条件变量，用于唤醒和hang IO
  std::condition_variable inflightcv_;

  // metacache存储当前文件的所有元数据信息
  MetaCache mc_;

  // IO最后由schedule模块向chunkserver端分发，scheduler由IOManager创建和释放
  RequestScheduler*     scheduler_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_IOCONTEXT_MANAGER_H
