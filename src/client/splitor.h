/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:20:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_SPLITOR_H
#define CURVE_LIBCURVE_SPLITOR_H

#include <list>

#include "src/client/metacache.h"
#include "src/client/io_tracker.h"
#include "src/client/config_info.h"
#include "src/client/request_context.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"

namespace curve {
namespace client {

class Splitor {
 public:
    static void Init(IOSplitOPtion_t ioSplitOpt);
    /**
     * 用户IO拆分成Chunk级别的IO
     * @param: iotracker大IO上下文信息
     * @param: mc是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: data是待读写的数据
     * @param: offset用户下发IO的其实偏移
     * @param: length数据长度
     * @param: mdsclient在查找metacahe失败时，通过mdsclient查找信息
     * @param: fi存储当前IO的一些基本信息，比如chunksize等
     */
    static int IO2ChunkRequests(IOTracker* iotracker,
                           MetaCache* mc,
                           std::list<RequestContext*>* targetlist,
                           const char* data,
                           off_t offset,
                           size_t length,
                           MDSClient* mdsclient,
                           const FInfo_t* fi);
    /**
     * 对单ChunkIO进行细粒度拆分
     * @param: iotracker大IO上下文信息
     * @param: mc是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: cid是当前chunk的ID信息
     * @param: data是待读写的数据
     * @param: offset是当前chunk内的偏移
     * @param: length数据长度
     * @param: seq是当前chunk的版本号
     */
    static int SingleChunkIO2ChunkRequests(IOTracker* iotracker,
                           MetaCache* mc,
                           std::list<RequestContext*>* targetlist,
                           const ChunkIDInfo_t cid,
                           const char* data,
                           off_t offset,
                           size_t length,
                           uint64_t seq);

 private:
    /**
     * IO2ChunkRequests内部会调用这个函数，进行真正的拆分操作
     * @param: iotracker大IO上下文信息
     * @param: mc是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: data是待读写的数据
     * @param: offset用户下发IO的其实偏移
     * @param: length数据长度
     * @param: mdsclient在查找metacahe失败时，通过mdsclient查找信息
     * @param: fi存储当前IO的一些基本信息，比如chunksize等
     * @param: chunkidx是当前chunk在vdisk中的索引值
     */
    static bool AssignInternal(IOTracker* iotracker,
                           MetaCache* mc,
                           std::list<RequestContext*>* targetlist,
                           const char* data,
                           off_t offset,
                           uint64_t length,
                           MDSClient* mdsclient,
                           const FInfo_t* fi,
                           ChunkIndex chunkidx);

 private:
    // IO拆分模块所使用的配置信息
    static IOSplitOPtion_t iosplitopt_;
};
}   // namespace client
}   // namespace curve
#endif
