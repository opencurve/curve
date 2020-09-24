/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Monday, 17th September 2018 3:22:06 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_IO_TRACKER_H_
#define SRC_CLIENT_IO_TRACKER_H_

#include <butil/iobuf.h>

#include <atomic>
#include <string>
#include <vector>

#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"
#include "src/client/request_context.h"
#include "include/client/libcurve.h"
#include "src/client/request_scheduler.h"
#include "include/curve_compiler_specific.h"
#include "src/client/io_condition_varaiable.h"

#include "proto/chunk.pb.h"

namespace curve {
namespace client {
class IOManager;

// IOTracker用于跟踪一个用户IO，因为一个用户IO可能会跨chunkserver，
// 因此在真正下发的时候会被拆分成多个小IO并发的向下发送，因此我们需要
// 跟踪发送的request的执行情况。
class CURVE_CACHELINE_ALIGNMENT IOTracker {
 public:
    /**
     * 构造函数
     * @param: iomanager负责回收当前iotracker
     * @param: mc用于获取chunk信息
     * @param: scheduler用于分发请求
     */
    IOTracker(IOManager* iomanager,
              MetaCache* mc,
              RequestScheduler* scheduler,
              FileMetric* clientMetric = nullptr);
    ~IOTracker() = default;

    /**
     * @brief StartRead同步读
     * @param buf 读缓冲区
     * @param offset 读偏移
     * @param length 读长度
     * @param mdsclient 透传给splitor，与mds通信
     * @param fileInfo 当前io对应文件的基本信息
     */
    void StartRead(void* buf, off_t offset, size_t length, MDSClient* mdsclient,
                   const FInfo_t* fileInfo);

    /**
     * @brief StartWrite同步写
     * @param buf 写缓冲区
     * @param offset 写偏移
     * @param length 写长度
     * @param mdsclient 透传给splitor，与mds通信
     * @param fileInfo 当前io对应文件的基本信息
     */
    void StartWrite(const void* buf, off_t offset, size_t length,
                    MDSClient* mdsclient, const FInfo_t* fileInfo);

    /**
     * @brief start an async read operation
     * @param ctx async read context
     * @param mdsclient used to communicate with MDS
     * @param fileInfo current file info
     */
    void StartAioRead(CurveAioContext* ctx, MDSClient* mdsclient,
                      const FInfo_t* fileInfo);

    /**
     * @brief start an async write operation
     * @param ctx async write context
     * @param mdsclient used to communicate with MDS
     * @param fileInfo current file info
     */
    void StartAioWrite(CurveAioContext* ctx, MDSClient* mdsclient,
                       const FInfo_t* fileInfo);
    /**
     * chunk相关接口是提供给snapshot使用的，上层的snapshot和file
     * 接口是分开的，在IOTracker这里会将其统一，这样对下层来说不用
     * 感知上层的接口类别。
     * @param:chunkidinfo 目标chunk
     * @param: seq是快照版本号
     * @param: offset是快照内的offset
     * @param: len是要读取的长度
     * @param: buf是读取缓冲区
     * @param: scc是异步回调
     */
    void ReadSnapChunk(const ChunkIDInfo &cinfo,
                     uint64_t seq,
                     uint64_t offset,
                     uint64_t len,
                     char *buf,
                     SnapCloneClosure* scc);
    /**
     * 删除此次转储时产生的或者历史遗留的快照
     * 如果转储过程中没有产生快照，则修改chunk的correctedSn
     * @param:chunkidinfo 目标chunk
     * @param: seq是需要修正的版本号
     */
    void DeleteSnapChunkOrCorrectSn(const ChunkIDInfo &cinfo,
                     uint64_t correctedSeq);
    /**
     * 获取chunk的版本信息，chunkInfo是出参
     * @param:chunkidinfo 目标chunk
     * @param: chunkInfo是快照的详细信息
     */
    void GetChunkInfo(const ChunkIDInfo &cinfo,
                     ChunkInfoDetail *chunkInfo);

    /**
     * @brief lazy 创建clone chunk
     * @param:location 数据源的url
     * @param:chunkidinfo 目标chunk
     * @param:sn chunk的序列号
     * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
     * @param:chunkSize chunk的大小
     * @param: scc是异步回调
     */
    void CreateCloneChunk(const std::string& location,
                          const ChunkIDInfo& chunkidinfo, uint64_t sn,
                          uint64_t correntSn, uint64_t chunkSize,
                          SnapCloneClosure* scc);

    /**
     * @brief 实际恢复chunk数据
     * @param:chunkidinfo chunkidinfo
     * @param:offset 偏移
     * @param:len 长度
     * @param:chunkSize chunk的大小
     * @param: scc是异步回调
     */
    void RecoverChunk(const ChunkIDInfo& chunkIdInfo, uint64_t offset,
                      uint64_t len, SnapCloneClosure* scc);

    /**
     * Wait用于同步接口等待，因为用户下来的IO被client内部线程接管之后
     * 调用就可以向上返回了，但是用户的同步IO语意是要等到结果返回才能向上
     * 返回的，因此这里的Wait会让用户线程等待。
     * @return: 返回读写信息，异步IO的时候返回0或-1.0代表成功，-1代表失败
     *          同步IO返回length或-1，length代表真实读写长度，-1代表读写失败
     */
    int  Wait();

    /**
     * 每个request都要有自己的OP类型，这里提供接口可以在io拆分的时候获取类型
     */
    OpType Optype() {return type_;}

    // 设置操作类型，测试使用
    void SetOpType(OpType type) { type_ = type; }

    /**
     * 因为client的IO都是异步发送的，且一个IO被拆分成多个Request，因此在异步
     * IO返回后就应该告诉IOTracker当前request已经返回，这样tracker可以处理
     * 返回的request。
     * @param: 待处理的异步request
     */
    void HandleResponse(RequestContext* reqctx);

    /**
     * 获取当前tracker id信息
     */
    uint64_t GetID() const {
        return id_;
    }

    // set user data type
    void SetUserDataType(const UserDataType dataType) {
        userDataType_ = dataType;
    }

    /**
     * @brief prepare space to store read data
     * @param subIoCount #space to store read data
     */
    void PrepareReadIOBuffers(const uint32_t subIoCount) {
        readDatas_.resize(subIoCount);
    }

    void SetReadData(const uint32_t subIoIndex, const butil::IOBuf& data) {
        readDatas_[subIoIndex] = data;
    }

 private:
    /**
     * 当IO返回的时候调用done，由done负责向上返回
     */
    void Done();

    /**
     * 在io拆分或者，io分发失败的时候需要调用，设置返回状态，并向上返回
     */
    void ReturnOnFail();
    /**
     * 用户下来的大IO会被拆分成多个子IO，这里在返回之前将子IO资源回收
     */
    void DestoryRequestList();

    /**
     * 填充request context common字段
     * @param: idinfo为chunk的id信息
     * @param: req为待填充的request context
     */
    void FillCommonFields(ChunkIDInfo idinfo, RequestContext* req);

    /**
     * chunkserver errcode转化为libcurve client的errode
     * @param: errcode为chunkserver侧的errode
     * @param[out]: errout为libcurve自己的errode
     */
    void ChunkServerErr2LibcurveErr(curve::chunkserver::CHUNK_OP_STATUS errcode,
                                    LIBCURVE_ERROR* errout);

    /**
     * 获取一个初始化后的RequestContext
     * return: 如果分配失败或者初始化失败，返回nullptr
     *         反之，返回一个指针
     */
    RequestContext* GetInitedRequestContext() const;

    // perform read operation
    void DoRead(MDSClient* mdsclient, const FInfo_t* fileInfo);

    // perform write operation
    void DoWrite(MDSClient* mdsclient, const FInfo_t* fileInfo);

 private:
    // io 类型
    OpType  type_;

    // 当前IO的数据内容，data是读写数据的buffer
    off_t      offset_;
    uint64_t   length_;

    // user data pointer
    void* data_;

    // user data type
    UserDataType userDataType_;

    // save write data
    butil::IOBuf writeData_;

    // save read data
    std::vector<butil::IOBuf> readDatas_;

    // 当用户下发的是同步IO的时候，其需要在上层进行等待，因为client的
    // IO发送流程全部是异步的，因此这里需要用条件变量等待，待异步IO返回
    // 之后才将这个等待的条件变量唤醒，然后向上返回。
    IOConditionVariable  iocv_;

    // 异步IO的context，在异步IO返回时，通过调用aioctx
    // 的异步回调进行返回。
    CurveAioContext* aioctx_;

    // 当前IO的errorcode
    LIBCURVE_ERROR errcode_;

    // 当前IO被拆分成reqcount_个小IO
    std::atomic<uint32_t> reqcount_;

    // 大IO被拆分成多个request，这些request放在reqlist中国保存
    std::vector<RequestContext*>   reqlist_;

    // scheduler用来将用户线程与client自己的线程切分
    // 大IO被切分之后，将切分的reqlist传给scheduler向下发送
    RequestScheduler* scheduler_;

    // metacache为当前fileinstance的元数据信息
    MetaCache* mc_;

    // 对于异步IO，Tracker需要向上层通知当前IO已经处理结束
    // iomanager可以将该tracker释放
    IOManager* iomanager_;

    // 发起时间
    uint64_t opStartTimePoint_;

    // client端的metric统计信息
    FileMetric* fileMetric_;

    // 当前tracker的id
    uint64_t id_;

    // 快照克隆系统异步调用回调指针
    SnapCloneClosure* scc_;

    // id生成器
    static std::atomic<uint64_t> tracekerID_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_IO_TRACKER_H_
