/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:15:27 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CLIENT_IOMANAGER4FILE_H_
#define SRC_CLIENT_IOMANAGER4FILE_H_

#include <string>
#include <atomic>
#include <mutex>  // NOLINT
#include <condition_variable>   // NOLINT

#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/client/metacache.h"
#include "src/client/iomanager.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"
#include "src/client/request_scheduler.h"
#include "include/curve_compiler_specific.h"
#include "src/client/inflight_controller.h"

using curve::common::Atomic;

namespace curve {
namespace client {
class FlightIOGuard;
class IOManager4File : public IOManager {
 public:
  IOManager4File();
  ~IOManager4File() = default;

  /**
   * 初始化函数
   * @param: filename为当前iomanager服务的文件名
   * @param: ioopt为当前iomanager的配置信息
   * @param: mdsclient向下透传给metacache
   * @return: 成功true,失败false
   */
  bool Initialize(const std::string& filename,
                  const IOOption_t& ioOpt,
                  MDSClient* mdsclient);

  /**
   * 同步模式读
   * @param: buf为当前待读取的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @return: 成功返回读取真实长度，-1为失败
   */
  int Read(char* buf, off_t offset, size_t length, MDSClient* mdsclient);
  /**
   * 同步模式写
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: buf为当前待写入的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return： 成功返回写入真实长度，-1为失败
   */
  int Write(const char* buf, off_t offset, size_t length, MDSClient* mdsclient);
  /**
   * 异步模式读
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return： 0为成功，小于0为失败
   */
  int AioRead(CurveAioContext* aioctx,
                      MDSClient* mdsclient);
  /**
   * 异步模式写
   * @param: mdsclient透传给底层，在必要的时候与mds通信
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return： 0为成功，小于0为失败
   */
  int AioWrite(CurveAioContext* aioctx,
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

  /**
   * 获取metric信息，测试代码使用
   */
  FileMetric_t* GetMetric() {
    return fileMetric_;
  }

  /**
   * 重新设置io配置信息，测试使用
   */
  void SetIOOpt(const IOOption_t& opt) {
     ioopt_ = opt;
  }

  /**
   * 测试使用，获取request scheduler
   */
  RequestScheduler* GetScheduler() { return scheduler_; }

  /**
   * lease excutor在检查到版本更新的时候，需要通知iomanager更新文件版本信息
   * @param: fi为当前需要更新的文件信息
   */
  void UpdataFileInfo(const FInfo_t& fi);

 private:
  friend class LeaseExcutor;
  friend class FlightIOGuard;
  /**
   * lease相关接口，当leaseexcutor续约失败的时候，调用LeaseTimeoutDisableIO
   * 将新下发的IO全部失败返回
   */
  void LeaseTimeoutBlockIO();

  /**
   * 当lease又续约成功的时候，leaseexcutor调用该接口恢复IO
   */
  void RefeshSuccAndResumeIO();

  /**
   * 当lesaeexcutor发现版本变更，调用该接口开始等待inflight回来，这段期间IO是hang的
   */
  void BlockIO();

  /**
   * 因为curve client底层都是异步IO，每个IO会分配一个IOtracker跟踪IO
   * 当这个IO做完之后，底层需要告知当前io manager来释放这个IOTracker，
   * HandleAsyncIOResponse负责释放IOTracker
   * @param: iotracker是返回的异步io
   */
  void HandleAsyncIOResponse(IOTracker* iotracker) override;

  class FlightIOGuard {
   public:
    explicit FlightIOGuard(IOManager4File* iomana) {
      iomanager = iomana;
      iomanager->inflightCntl_.IncremInflightNum();
    }

    ~FlightIOGuard() {
      iomanager->inflightCntl_.DecremInflightNum();
    }

   private:
    IOManager4File* iomanager;
  };

 private:
  // 当前文件的信息
  FInfo_t fi_;

  // 每个IOManager都有其IO配置，保存在iooption里
  IOOption_t  ioopt_;

  // metacache存储当前文件的所有元数据信息
  MetaCache mc_;

  // IO最后由schedule模块向chunkserver端分发，scheduler由IOManager创建和释放
  RequestScheduler*     scheduler_;

  // client端metric统计信息
  FileMetric_t*        fileMetric_;

  // task thread pool为了将qemu线程与curve线程隔离
  curve::common::TaskThreadPool taskPool_;

  // inflight IO控制
  InflightControl  inflightCntl_;

  // 是否退出
  bool exit_;

  // lease续约线程与qemu一侧线程调用是并发的
  // qemu在调用close的时候会关闭iomanager及其对应
  // 资源。lease续约线程在续约成功或失败的时候会通知iomanager的
  // scheduler线程现在需要block IO或者resume IO，所以
  // 如果在lease续约线程需要通知iomanager的时候，这时候
  // 如果iomanager的资源scheduler已经被释放了，就会
  // 导致crash，所以需要对这个资源加一把锁，在退出的时候
  // 不会有并发的情况，保证在资源被析构的时候lease续约
  // 线程不会再用到这些资源.
  std::mutex exitMtx_;
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_IOMANAGER4FILE_H_
