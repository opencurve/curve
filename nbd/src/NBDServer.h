/**
 * Project : curve
 * Date : Wed Apr 22 17:04:51 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#ifndef SRC_TOOLS_NBD_NBDSERVER_H_
#define SRC_TOOLS_NBD_NBDSERVER_H_

#include <linux/nbd.h>
#include <atomic>
#include <condition_variable>  // NOLINT
#include <deque>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "nbd/src/ImageInstance.h"
#include "nbd/src/NBDController.h"
#include "nbd/src/SafeIO.h"
#include "nebd/src/part1/libnebd.h"

namespace curve {
namespace nbd {

class NBDServer;

// NBD IO请求上下文信息
struct IOContext {
    struct nbd_request request;
    struct nbd_reply reply;

    // 请求类型
    int command = 0;

    NBDServer* server = nullptr;
    std::unique_ptr<char[]> data;

    // NEBD请求上下文信息
    NebdClientAioContext nebdAioCtx;

    IOContext() {
        memset(&nebdAioCtx, 0, sizeof(nebdAioCtx));
    }
};

// NBDServer负责与nbd内核进行数据通信
class NBDServer {
 public:
    NBDServer(int sock, NBDControllerPtr nbdCtrl,
              std::shared_ptr<ImageInstance> imageInstance,
              std::shared_ptr<SafeIO> safeIO = std::make_shared<SafeIO>())
        : started_(false),
          terminated_(false),
          sock_(sock),
          nbdCtrl_(nbdCtrl),
          image_(imageInstance),
          pendingRequestCounts_(0),
          safeIO_(safeIO) {}

    ~NBDServer();

    /**
     * @brief 启动server
     */
    void Start();

    /**
     * 等待断开连接
     */
    void WaitForDisconnect();

    /**
     * 测试使用，返回server是否终止
     */
    bool IsTerminated() const {
        return terminated_;
    }

    NBDControllerPtr GetController() {
        return nbdCtrl_;
    }

 private:
    /**
     * @brief NEBD异步请求回调函数
     */
    static void NBDAioCallback(struct NebdClientAioContext* context);

    /**
     * @brief 关闭server
     */
    void Shutdown();

    /**
     * 等下已下发请求全部返回
     */
    void WaitClean();

    /**
     * @brief 读线程执行函数
     */
    void ReaderFunc();

    /**
     * @brief 写线程执行函数
     */
    void WriterFunc();

    /**
     * @brief 异步请求开始时执行函数
     */
    void OnRequestStart();

    /**
     * @brief 异步请求结束时执行函数
     * @param ctx 异步请求上下文
     */
    void OnRequestFinish(IOContext* ctx);

    /**
     * @brief 等待异步请求返回
     * @return 异步请求context
     */
    IOContext* WaitRequestFinish();

    /**
     * 发起异步请求
     * @param ctx nbd请求上下文
     * @return 请求是否发起成功
     */
    bool StartAioRequest(IOContext* ctx);

 private:
    // server是否启动
    std::atomic<bool> started_;
    // server是否停止
    std::atomic<bool> terminated_;

    // 与内核通信的socket fd
    int sock_;
    NBDControllerPtr nbdCtrl_;
    std::shared_ptr<ImageInstance> image_;
    std::shared_ptr<SafeIO> safeIO_;

    // 保护pendingRequestCounts_和finishedRequests_
    std::mutex requestMtx_;
    std::condition_variable requestCond_;

    // 正在执行过程中的请求数量
    uint64_t pendingRequestCounts_;

    // 已完成请求上下文队列
    std::deque<IOContext*> finishedRequests_;

    // 读线程
    std::thread readerThread_;
    // 写线程
    std::thread writerThread_;

    // 等待断开连接锁/条件变量
    std::mutex disconnectMutex_;
    std::condition_variable disconnectCond_;
};
using NBDServerPtr = std::shared_ptr<NBDServer>;

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_NBDSERVER_H_
