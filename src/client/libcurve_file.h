/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:46:54 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_LIBCURVE_FILE_H
#define CURVE_LIBCURVE_FILE_H

#include <unistd.h>
#include <string>
#include <atomic>
#include <unordered_map>

#include "src/common/concurrent/rw_lock.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "src/client/libcurve_define.h"
#include "src/client/file_instance.h"

using curve::common::RWLock;

// TODO(tongguangxun) :添加关键函数trace功能
namespace curve {
namespace client {
// FileClient是vdisk的管理类，一个QEMU对应多个vdisk

class FileClient {
 public:
  FileClient();
  ~FileClient() = default;

  /**
   * file对象初始化函数
   * @param: 配置文件路径
   */
  LIBCURVE_ERROR Init(const char* configpath);
  /**
   * 打开或创建文件
   * @param: filename文件名
   * @param: userinfo是当前打开或创建时携带的user信息
   * @param: size文件长度，当create为true的时候以size长度创建文件
   * @param: create为true，文件不存在就创建
   * @return: 返回文件fd
   */
  int Open(std::string filename,
           UserInfo_t userinfo,
           size_t size = 0,
           bool create = false);
  /**
   * 同步模式读
   * @param: fd为当前open返回的文件描述符
   * @param: buf为当前待读取的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR Read(int fd, char* buf, off_t offset, size_t length);
  /**
   * 同步模式写
   * @param: fd为当前open返回的文件描述符
   * @param: buf为当前待写入的缓冲区
   * @param：offset文件内的便宜
   * @parma：length为待读取的长度
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR Write(int fd, const char* buf, off_t offset, size_t length);
  /**
   * 异步模式读
   * @param: fd为当前open返回的文件描述符
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR AioRead(int fd, CurveAioContext* aioctx);
  /**
   * 异步模式写
   * @param: fd为当前open返回的文件描述符
   * @param: aioctx为异步读写的io上下文，保存基本的io信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR AioWrite(int fd, CurveAioContext* aioctx);
  /**
   * 获取文件信息
   * @param: fd为当前open返回的文件描述符
   * @param: filename为文件名
   * @param: finfo是出参，携带当前文件的基础信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR StatFs(int fd, std::string filename, FileStatInfo* finfo);
  /**
   * close通过fd找到对应的instance进行删除
   * @param: fd为当前open返回的文件描述符
   */
  void Close(int fd);
  /**
   * 析构，回收资源
   */
  void UnInit();

 private:
  RWLock rwlock_;

  // 向上返回的文件描述符，对于QEMU来说，一个vdisk对应一个文件描述符
  std::atomic<uint64_t>    fdcount_;

  // 每个vdisk都有一个FileInstance，通过返回的fd映射到对应的instance
  std::unordered_map<int, FileInstance*> fileserviceMap_;

  // FileClient配置
  ClientConfig clientconfig_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_H
