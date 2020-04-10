/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:46:54 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_LIBCURVE_FILE_H_
#define SRC_CLIENT_LIBCURVE_FILE_H_

#include <unistd.h>
#include <string>
#include <atomic>
#include <unordered_map>
#include <vector>

#include "include/client/libcurve.h"
#include "src/common/uuid.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "src/client/file_instance.h"

using curve::common::BthreadRWLock;

// TODO(tongguangxun) :添加关键函数trace功能
namespace curve {
namespace client {

void InitLogging(const std::string& confpath);

class LoggerGuard {
 public:
  explicit LoggerGuard(const std::string& confpath) {
    InitInternal(confpath);
  }

  ~LoggerGuard() {
    google::ShutdownGoogleLogging();
  }

 private:
  static void InitInternal(const std::string& confpath);
};

// FileClient是vdisk的管理类，一个QEMU对应多个vdisk
class FileClient {
 public:
  FileClient();
  virtual ~FileClient() = default;

  /**
   * file对象初始化函数
   * @param: 配置文件路径
   */
  virtual int Init(const std::string& configpath);

  /**
   * 打开或创建文件
   * @param: filename文件名
   * @param: userinfo是操作文件的用户信息
   * @return: 返回文件fd
   */
  virtual int Open(const std::string& filename,
                   const UserInfo_t& userinfo,
                   std::string* sessionId = nullptr);

  /**
   * 重新打开文件
   * @param: filename文件名
   * @param: sessionId是上次打开该文件时返回的sessionId
   * @param: userInfo是操作文件的用户信息
   * @return: 返回文件fd
   */

  virtual int ReOpen(const std::string& filename,
                     const std::string& sessionId,
                     const UserInfo& userInfo,
                     std::string* newSessionId);

  /**
   * 打开文件，这个打开只是创建了一个fd，并不与mds交互，没有session续约
   * 这个Open接口主要是提供给快照克隆镜像系统做数据拷贝使用
   * @param: filename文件名
   * @param: userinfo当前用户信息
   * @return: 返回文件fd
   */
  virtual int Open4ReadOnly(const std::string& filename,
                                const UserInfo_t& userinfo);

  /**
   * 创建文件
   * @param: filename文件名
   * @param: userinfo是当前打开或创建时携带的user信息
   * @param: size文件长度，当create为true的时候以size长度创建文件
   * @return: 成功返回0, 失败可能有多种可能
   *          比如内部错误，或者文件已存在
   */
  virtual int Create(const std::string& filename,
             const UserInfo_t& userinfo,
             size_t size);

  /**
   * 同步模式读
   * @param: fd为当前open返回的文件描述符
   * @param: buf为当前待读取的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return: 成功返回读取字节数,否则返回小于0的错误码
   */
  virtual int Read(int fd, char* buf, off_t offset, size_t length);

  /**
   * 同步模式写
   * @param: fd为当前open返回的文件描述符
   * @param: buf为当前待写入的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return: 成功返回写入字节数,否则返回小于0的错误码
   */
  virtual int Write(int fd, const char* buf, off_t offset, size_t length);

  /**
   * 异步模式读
   * @param: fd为当前open返回的文件描述符
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return: 成功返回读取字节数,否则返回小于0的错误码
   */
  virtual int AioRead(int fd, CurveAioContext* aioctx);

  /**
   * 异步模式写
   * @param: fd为当前open返回的文件描述符
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return: 成功返回写入字节数,否则返回小于0的错误码
   */
  virtual int AioWrite(int fd, CurveAioContext* aioctx);

  /**
   * 重命名文件
   * @param: userinfo是用户信息
   * @param: oldpath源路劲
   * @param: newpath目标路径
   */
  virtual int Rename(const UserInfo_t& userinfo,
                     const std::string& oldpath,
                     const std::string& newpath);

  /**
   * 扩展文件
   * @param: userinfo是用户信息
   * @param: filename文件名
   * @param: newsize新的size
   */
  virtual int Extend(const std::string& filename,
                     const UserInfo_t& userinfo,
                     uint64_t newsize);

  /**
   * 删除文件
   * @param: userinfo是用户信息
   * @param: filename待删除的文件名
   * @param: deleteforce=true只能用于从回收站删除,false为放入垃圾箱
   */
  virtual int Unlink(const std::string& filename,
                     const UserInfo_t& userinfo,
                     bool deleteforce = false);

  /**
   * 枚举目录内容
   * @param: userinfo是用户信息
   * @param: dirpath是目录路径
   * @param[out]: filestatVec当前文件夹内的文件信息
   */
  virtual int Listdir(const std::string& dirpath,
                      const UserInfo_t& userinfo,
                      std::vector<FileStatInfo>* filestatVec);

  /**
   * 创建目录
   * @param: userinfo是用户信息
   * @param: dirpath是目录路径
   */
  virtual int Mkdir(const std::string& dirpath, const UserInfo_t& userinfo);

  /**
   * 删除目录
   * @param: userinfo是用户信息
   * @param: dirpath是目录路径
   */
  virtual int Rmdir(const std::string& dirpath, const UserInfo_t& userinfo);

  /**
   * 获取文件信息
   * @param: filename文件名
   * @param: userinfo是用户信息
   * @param: finfo是出参，携带当前文件的基础信息
   * @return: 成功返回int::OK,否则返回小于0的错误码
   */
  virtual int StatFile(const std::string& filename,
                       const UserInfo_t& userinfo,
                       FileStatInfo* finfo);

  /**
   * 获取文件信息
   * @param: filename文件名
   * @param: userinfo是用户信息
   * @param: finfo2是出参，携带当前文件的基础信息,包括文件状态
   * @return: 成功返回int::OK,否则返回小于0的错误码
   */
  virtual int StatFile2(const std::string& filename, const UserInfo_t& userinfo,
                        FileStatInfo2* finfo);

  /**
   * 变更owner
   * @param: filename待变更的文件名
   * @param: newOwner新的owner信息
   * @param: userinfo执行此操作的user信息，只有root用户才能执行变更
   * @return: 成功返回0，
   *          否则返回-LIBCURVE_ERROR::FAILED,-LIBCURVE_ERROR::AUTHFAILED等
   */
  virtual int ChangeOwner(const std::string& filename,
                      const std::string& newOwner,
                      const UserInfo_t& userinfo);
  /**
   * close通过fd找到对应的instance进行删除
   * @param: fd为当前open返回的文件描述符
   * @return: 成功返回int::OK,否则返回小于0的错误码
   */
  virtual int Close(int fd);

  /**
   * 析构，回收资源
   */
  virtual void UnInit();

  /**
   * @brief: 获取集群id
   * @param: buf存放集群id
   * @param: buf的长度
   * @return: 成功返回0, 失败返回-LIBCURVE_ERROR::FAILED
   */
  int GetClusterId(char* buf, int len);

  /**
   * @brief 获取文件信息，测试使用
   * @param fd 文件句柄
   * @param[out] finfo 文件信息
   * @return 成功返回0，失败返回-LIBCURVE_ERROR::FAILED
   */
  int GetFileInfo(int fd, FInfo* finfo);

 private:
  inline bool CheckAligned(off_t offset, size_t length);

  // 获取一个初始化的FileInstance对象
  // return: 成功返回指向对象的指针,否则返回nullptr
  FileInstance* GetInitedFileInstance(const std::string& filename,
                                const UserInfo& userinfo,
                                bool readonly);

 private:
  BthreadRWLock rwlock_;

  // 向上返回的文件描述符，对于QEMU来说，一个vdisk对应一个文件描述符
  std::atomic<uint64_t>    fdcount_;

  // 每个vdisk都有一个FileInstance，通过返回的fd映射到对应的instance
  std::unordered_map<int, FileInstance*> fileserviceMap_;

  // FileClient配置
  ClientConfig clientconfig_;

  // fileclient对应的全局mdsclient
  MDSClient* mdsClient_;

  // 是否初始化成功
  bool  inited_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_LIBCURVE_FILE_H_
