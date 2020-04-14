/*
 * Project: curve
 * Created Date: Saturday March 9th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#ifndef SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_
#define SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_

#include <string>
#include <vector>
#include "include/etcdclient/etcdclient.h"

namespace curve {
namespace kvstorage {
class KVStorageClient {
 public:
    KVStorageClient() {}
    virtual ~KVStorageClient() {}

    /**
     * @brief Put 存储key-value
     *
     * @param[in] key
     * @param[in] value
     *
     * @return 返回错误码EtcdErrCode
     */
    virtual int Put(const std::string &key, const std::string &value) = 0;

    /**
     * @brief PutRewithRevision 存储key-value
     *
     * @param[in] key
     * @param[in] value
     * @param[out] revision 返回版本号
     *
     * @return 返回错误码EtcdErrCode
     */
    virtual int PutRewithRevision(const std::string &key,
        const std::string &value, int64_t *revision) = 0;

    /**
     * @brief Get 获取指定key的value
     *
     * @param[in] key
     * @param[out] get到的value值
     *
     * @return 错误码
     */
    virtual int Get(const std::string &key, std::string *out) = 0;

    /**
     * @brief List 获取[startKey， endKey)之间所有的value值
     *
     * @param[in] startKey
     * @param[in] endKey
     * @param[out] values endKey的长度
     *
     * @return 错误码
     */
    virtual int List(const std::string &startKey, const std::string &endKey,
        std::vector<std::string> *values) = 0;

    /**
     * @brief Delete 删除指定key的value
     *
     * @param[in] key
     *
     * @return 返回错误码
     */
    virtual int Delete(const std::string &key) = 0;

    /**
     * @brief DeleteRewithRevision 删除指定key的value
     *
     * @param[in] key
     * @param[out] revision 返回版本号
     *
     * @return 返回错误码
     */
    virtual int DeleteRewithRevision(
        const std::string &key, int64_t *revision) = 0;

    /*
    * @brief TxnN 事务 按照ops[0] ops[1] ... 的顺序进行操作，目前支持2个和3个操作
    *
    * @param[in] ops 操作集合
    *
    * @return 错误码
    */
    virtual int TxnN(const std::vector<Operation> &ops) = 0;

    /**
     * @brief CompareAndSwap 事务，实现CAS
     *
     * @param[in] key
     * @param[in] preV 需要满足的value条件
     * @param[in] target 新的value
     *
     * @return 错误码
     */
    virtual int CompareAndSwap(const std::string &key, const std::string &preV,
        const std::string &target) = 0;
};

// 封装go编译生成的etcd的c头文件
class EtcdClientImp : public KVStorageClient {
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

    int Put(const std::string &key, const std::string &value) override;

    int PutRewithRevision(const std::string &key, const std::string &value,
        int64_t *revision) override;

    int Get(const std::string &key, std::string *out) override;

    int List(const std::string &startKey,
        const std::string &endKey, std::vector<std::string> *values) override;

    int Delete(const std::string &key) override;

    int DeleteRewithRevision(
        const std::string &key, int64_t *revision) override;

    int TxnN(const std::vector<Operation> &ops) override;

    int CompareAndSwap(const std::string &key, const std::string &preV,
        const std::string &target) override;

    virtual int GetCurrentRevision(int64_t *revision);

    /**
     * @brief ListWithLimitAndRevision
     *        获取指定个数，指定revision，[startKey, endKey)的键值对
     *
     * @param[in] startKey 起始key
     * @param[in] endKey 终止key, 不包含
     * @param[in] limit 最多个数
     * @param[in] revision 获取<=revision的key
     * @param[out] values 所有键值对的value集合
     * @param[out] lastKey 集合的最后一个key值
     */
    virtual int ListWithLimitAndRevision(const std::string &startKey,
        const std::string &endKey, int64_t limit, int64_t revision,
        std::vector<std::string> *values, std::string *lastKey);

    /**
     * @brief CampaignLeader 通过etcd竞选leader,如果成功则返回; 如果未竞选成功，
     *      electionTimeoutMs>0的情况下会返回失败， electionTimeoutMs=0的情况
     *      会一直block住，直到当选leader或者出现fatal error
     *
     * @param[in] pfx 竞选leader要写的key
     * @param[in] leaderName leader名字，建议使用ip+port的形式表示唯一
     * @param[in] sessionInterSec 用于创建带ttl的session, 如果client挂掉,
     *                            esession超时后，etcd会把该leader注册的节点删除
     * @param[in] electionTimeoutMs 该接口的超时时间，为0会一直block
     * @param[out] leaderOid leader的objectId，记录在objectManager中
     *
     * @return EtcdErrCode::EtcdCampaignLeaderSuccess表示成功，其他表示失败
     */
    virtual int CampaignLeader(
        const std::string &pfx, const std::string &leaderName,
        uint32_t sessionInterSec, uint32_t electionTimeoutMs,
        uint64_t *leaderOid);

    /**
     * @brief LeaderObserve
     *        监听当前mds与etcd的session情况，如果session关闭或者过期会返回错误
     *
     * @param[in] leaderOid 监测指定leader
     * @param[in] leaderName leader竞选时用的名称
     *
     * @return 如果函数返回，mds与etcd之间的session失效
     */
    virtual int LeaderObserve(
        uint64_t leaderOid, const std::string &leaderName);

    /**
     * @brief LeaderResign leader主动卸任leader，成功后其他节点可以竞选leader
     *
     * @param[in] leaderOid 指定leader
     * @param[in] timeoutMs client的get/txn等操作的超时时间
     *
     * @return EtcdErrCode::EtcdLeaderResiginSuccess卸任成功
     *         EtcdErrCode::EtcdLeaderResiginErr卸任失败
     */
     virtual int LeaderResign(uint64_t leaderOid, uint64_t timeoutMs);

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
}  // namespace kvstorage
}  // namespace curve

#endif  // SRC_KVSTORAGECLIENT_ETCD_CLIENT_H_


