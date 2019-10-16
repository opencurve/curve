/*
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_PART1_NEBD_CLIENT_H_
#define SRC_PART1_NEBD_CLIENT_H_

#include <brpc/channel.h>
#include <string>
#include "src/part1/libnebd.h"
#include "src/common/configuration.h"
#include "src/part1/nebd_lifecycle.h"
#include "src/common/client.pb.h"

namespace nebd {
namespace client {

class FileClient {
 public:
    static FileClient &GetInstance() {
        static FileClient fileClient;
        return fileClient;
    }
    ~FileClient() = default;
    /**
     *  @brief 初始化nebd，仅在第一次调用的时候真正执行初始化逻辑
     *  @param none
     *  @return 成功返回0，失败返回-1
     */
    int Init(const char* confpath);
    /**
     *  @brief 反初始化nebd
     *  @param none
     *  @return 成功返回0，失败返回-1
     */
    void Uninit();
    /**
     *  @brief open文件
     *  @param filename：文件名
     *  @return 成功返回文件fd，失败返回错误码
     */
    int Open(const char* filename);
    /**
     *  @brief close文件
     *  @param fd：文件的fd
     *  @return 成功返回0，失败返回错误码
     */
    int Close(int fd);
    /**
     *  @brief resize文件
     *  @param fd：文件的fd
     *         size：调整后的文件size
     *  @return 成功返回0，失败返回错误码
     */
    int Extend(int fd, int64_t newsize);
    /**
     *  @brief 获取文件size
     *  @param fd：文件的fd
     *  @return 成功返回文件size，失败返回错误码
     */
    int64_t StatFile(int fd);
    /**
     *  @brief discard文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int Discard(int fd, ClientAioContext* aioctx);
    /**
     *  @brief 读文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int AioRead(int fd, ClientAioContext* aioctx);
    /**
     *  @brief 写文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int AioWrite(int fd, ClientAioContext* aioctx);
    /**
     *  @brief flush文件，异步函数
     *  @param fd：文件的fd
     *         context：异步请求的上下文，包含请求所需的信息以及回调
     *  @return 成功返回0，失败返回错误码
     */
    int Flush(int fd, ClientAioContext* aioctx);
    /**
     *  @brief 获取文件info
     *  @param fd：文件的fd
     *  @return 成功返回文件对象size，失败返回错误码
     */
    int64_t GetInfo(int fd);
    /**
     *  @brief 刷新cache，等所有异步请求返回
     *  @param fd：文件的fd
     *  @return 成功返回0，失败返回错误码
     */
    int InvalidCache(int fd);

    // for test
    int InitChannel(const std::string& addr);
    int LoadConf(common::Configuration *conf);

 private:
    //  int LoadConf(common::Configuration *conf);
    //  int InitChannel(const std::string& addr);

 private:
    brpc::Channel channel_;
    // 同步rpc请求的最大重试次数
    uint32_t rpcRetryTimes_;
    // 同步rpc请求的重试间隔；
    // 也用来计算异步rpc请求的重试间隔，也是异步rpc请求重试的最小间隔
    uint32_t rpcRetryIntervalUs_;
    // 当异步请求rpc返回EHOSTDOWN错误时的重试时间
    uint32_t rpcHostDownRetryIntervalUs_;
    // 用来计算rpc请求的重试间隔，也是异步rpc请求重试的最大间隔
    uint32_t rpcRetryMaxIntervalUs_;
    // rpc请求的超时时间
    uint32_t rpcTimeoutMs_;
    // brpc的健康检查周期时间
    uint32_t rpcHealthCheckIntervalS_;
    // 异步IO操作RPC失败日志打印间隔，每隔多少次打印一次
    uint32_t aioRpcFailLogInterval_;
    // 管理生命周期的服务
    nebd::client::LifeCycleManager lifeCycleManager_;
};

extern FileClient &fileClient;

class AsyncRequestDone: public google::protobuf::Closure {
 public:
    AsyncRequestDone(int fd, ClientAioContext* aioctx,
                     uint32_t rpcTimeoutMs,
                     uint32_t rpcRetryIntervalUs,
                     uint32_t rpcRetryMaxIntervalUs,
                     uint32_t rpcHostDownRetryIntervalUs):
                     fd_(fd),
                     aioctx_(aioctx),
                     rpcTimeoutMs_(rpcTimeoutMs),
                     rpcRetryIntervalUs_(rpcRetryIntervalUs),
                     rpcRetryMaxIntervalUs_(rpcRetryMaxIntervalUs),
                     rpcHostDownRetryIntervalUs_(rpcHostDownRetryIntervalUs) {
        cntl_.set_timeout_ms(rpcTimeoutMs);
    }

    /**
     * @brief 计算rpc的重试间隔，重试时间根据重试次数线性增加，
     *        最小不小于rpcRetryIntervalUs_，最大不大于rpcRetryMaxIntervalUs_
     * @param retryCount: 重试次数
     * @return 返回计算出来的重试间隔
     */
    uint32_t GetRpcRetryIntervalUs(uint32_t retryCount);

    virtual void Run() {}

    // 请求的fd
    int fd_;
    // 请求的上下文
    ClientAioContext* aioctx_;
    // 用来设置rpc请求的超时时间
    uint32_t rpcTimeoutMs_;
    // 用来计算rpc请求的重试间隔，也是异步rpc请求重试的最小间隔
    uint32_t rpcRetryIntervalUs_;
    // 用来计算rpc请求的重试间隔，也是异步rpc请求重试的最大间隔
    uint32_t rpcRetryMaxIntervalUs_;
    // 当异步请求rpc返回EHOSTDOWN错误时的重试时间
    uint32_t rpcHostDownRetryIntervalUs_;
    // 异步IO操作RPC失败日志打印间隔，每隔多少次打印一次
    uint32_t aioRpcFailLogInterval_;
    // brpc异步请求的controller，保存下来避免预期之外的析构
    brpc::Controller cntl_;
};

class ReadDone: public AsyncRequestDone {
 public:
    ReadDone(int fd, ClientAioContext* aioctx, uint32_t rpcTimeoutMs,
            uint32_t rpcRetryIntervalUs, uint32_t rpcRetryMaxIntervalUs,
            uint32_t rpcHostDownRetryIntervalUs):
            AsyncRequestDone(fd, aioctx, rpcTimeoutMs,
                            rpcRetryIntervalUs, rpcRetryMaxIntervalUs,
                            rpcHostDownRetryIntervalUs) {}
    void Run();
    nebd::client::ReadResponse response_;
};

class WriteDone: public AsyncRequestDone {
 public:
    WriteDone(int fd, ClientAioContext* aioctx, uint32_t rpcTimeoutMs,
            uint32_t rpcRetryIntervalUs, uint32_t rpcRetryMaxIntervalUs,
            uint32_t rpcHostDownRetryIntervalUs):
            AsyncRequestDone(fd, aioctx, rpcTimeoutMs,
                            rpcRetryIntervalUs, rpcRetryMaxIntervalUs,
                            rpcHostDownRetryIntervalUs) {}
    void Run();
    nebd::client::WriteResponse response_;
};

class DiscardDone: public AsyncRequestDone {
 public:
    DiscardDone(int fd, ClientAioContext* aioctx, uint32_t rpcTimeoutMs,
            uint32_t rpcRetryIntervalUs, uint32_t rpcRetryMaxIntervalUs,
            uint32_t rpcHostDownRetryIntervalUs):
            AsyncRequestDone(fd, aioctx, rpcTimeoutMs,
                            rpcRetryIntervalUs, rpcRetryMaxIntervalUs,
                            rpcHostDownRetryIntervalUs) {}
    void Run();
    nebd::client::DiscardResponse response_;
};

class FlushDone: public AsyncRequestDone {
 public:
    FlushDone(int fd, ClientAioContext* aioctx, uint32_t rpcTimeoutMs,
            uint32_t rpcRetryIntervalUs, uint32_t rpcRetryMaxIntervalUs,
            uint32_t rpcHostDownRetryIntervalUs):
            AsyncRequestDone(fd, aioctx, rpcTimeoutMs,
                            rpcRetryIntervalUs, rpcRetryMaxIntervalUs,
                            rpcHostDownRetryIntervalUs) {}
    void Run();
    nebd::client::FlushResponse response_;
};

}  // namespace client
}  // namespace nebd

#endif  // SRC_PART1_NEBD_CLIENT_H_
