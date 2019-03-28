/*
 * Project: curve
 * Created Date: Saturday March 9th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_ETCD_STORAGE_H_
#define SRC_MDS_NAMESERVER2_ETCD_STORAGE_H_

#include <string>
#include <vector>
#include "thirdparties/etcdclient/etcdclient.h"

namespace curve {
namespace mds {
// 封装go编译生成的etcd的c头文件
class StorageClient {
 public:
  StorageClient() {}
  virtual ~StorageClient() {}

  /**
   * @brief Put 存储key-value
   *
   * @param[in] key
   * @param[in] value
   *
   * @return 返回错误码EtcdErrCode
   */
  virtual int Put(std::string key, std::string value) = 0;

  /**
   * @brief Get 获取指定key的value
   *
   * @param[in] key
   * @param[out] get到的value值
   *
   * @return 错误码
   */
  virtual int Get(std::string key, std::string *out) = 0;

  /**
   * @brief List 获取[startKey， endKey)之间所有的value值
   *
   * @param[in] startKey
   * @param[in] endKey
   * @param[out] values endKey的长度
   *
   * @return 错误码
   */
  virtual int List(std::string startKey, std::string endKey,
    std::vector<std::string> *values) = 0;

  /**
   * @brief Delete 删除指定key的value
   *
   * @param[in] key
   *
   * @return 返回错误码
   */
  virtual int Delete(std::string key) = 0;

  /*
  * @brief List 事务 按传入顺序进行op1 op2的操作
  *
  * @param[in] op1 操作1
  * @param[in] op2 操作2
  *
  * @return 错误码
  */
  virtual int Txn2(Operation op1, Operation op2) = 0;

  /**
   * @brief CompareAndSwap 事务，实现CAS
   *
   * @param[in] key
   * @param[in] preV 需要满足的value条件
   * @param[in] target 新的value
   *
   * @return 错误码
   */
  virtual int CompareAndSwap(
      std::string key, std::string preV, std::string target) = 0;
};

class EtcdClientImp : public StorageClient {
 public:
  EtcdClientImp() {}
  ~EtcdClientImp() {
      CloseClient();
  }

  /**
   * @brief Init 初始化etcdclient, 是go中的一个全局变量
   *
   * @param[in] conf 初始化etcdclient需要的配置项
   * @param[in] timout 下列接口timeout的时间
   * @param[in] retryTimes 失败重试次数
   *
   * @return 返回错误码EtcdErrCode
   */
  int Init(EtcdConf conf, int timeout, int retryTiems);

  void CloseClient();

  int Put(std::string key, std::string value) override;

  int Get(std::string key, std::string *out) override;

  int List(std::string startKey,
    std::string endKey, std::vector<std::string> *values) override;

  int Delete(std::string key) override;

  int Txn2(Operation op1, Operation op2) override;

  int CompareAndSwap(
      std::string key, std::string preV, std::string target) override;

  // for test
  void SetTimeout(int time);

 private:
  bool NeedRetry(int errCode);

 private:
  // 每个接口的超时时间，单位是ms
  int timeout_;
  // 失败重试次数
  int retryTimes_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ETCD_STORAGE_H_


