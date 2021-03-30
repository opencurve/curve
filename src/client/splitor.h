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
 * File Created: Monday, 17th September 2018 4:20:52 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_SPLITOR_H_
#define SRC_CLIENT_SPLITOR_H_

#include <butil/iobuf.h>

#include <vector>
#include <string>

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
    static void Init(const IOSplitOption& ioSplitOpt);

    /**
     * 用户IO拆分成Chunk级别的IO
     * @param: iotracker大IO上下文信息
     * @param: metaCache是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: data 是待写的数据
     * @param: offset用户下发IO的其实偏移
     * @param: length数据长度
     * @param: mdsclient在查找metacahe失败时，通过mdsclient查找信息
     * @param: fi存储当前IO的一些基本信息，比如chunksize等
     */
    static int IO2ChunkRequests(IOTracker* iotracker,
                           MetaCache* metaCache,
                           std::vector<RequestContext*>* targetlist,
                           butil::IOBuf* data,
                           off_t offset,
                           size_t length,
                           MDSClient* mdsclient,
                           const FInfo_t* fi);

    /**
     * 对单ChunkIO进行细粒度拆分
     * @param: iotracker大IO上下文信息
     * @param: metaCache是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: cid是当前chunk的ID信息
     * @param: data是待写的数据
     * @param: offset是当前chunk内的偏移
     * @param: length数据长度
     * @param: seq是当前chunk的版本号
     */
    static int SingleChunkIO2ChunkRequests(IOTracker* iotracker,
                           MetaCache* metaCache,
                           std::vector<RequestContext*>* targetlist,
                           const ChunkIDInfo& cid,
                           butil::IOBuf* data,
                           off_t offset,
                           size_t length,
                           uint64_t seq);

    /**
     * @brief 计算请求的location信息
     * @param ioTracker io上下文信息
     * @param metaCache 文件缓存信息
     * @param chunkIdx 当前chunk信息
     * @return source信息
     */
    static RequestSourceInfo CalcRequestSourceInfo(IOTracker* ioTracker,
                                                   MetaCache* metaCache,
                                                   ChunkIndex chunkIdx);

 private:
    /**
     * IO2ChunkRequests内部会调用这个函数，进行真正的拆分操作
     * @param: iotracker大IO上下文信息
     * @param: mc是io拆分过程中需要使用的缓存信息
     * @param: targetlist大IO被拆分之后的小IO存储列表
     * @param: data 是待写的数据
     * @param: offset用户下发IO的其实偏移
     * @param: length数据长度
     * @param: mdsclient在查找metacahe失败时，通过mdsclient查找信息
     * @param: fi存储当前IO的一些基本信息，比如chunksize等
     * @param: chunkidx是当前chunk在vdisk中的索引值
     */
    static bool AssignInternal(IOTracker* iotracker,
                           MetaCache* metaCache,
                           std::vector<RequestContext*>* targetlist,
                           butil::IOBuf* data,
                           off_t offset,
                           uint64_t length,
                           MDSClient* mdsclient,
                           const FInfo_t* fi,
                           ChunkIndex chunkidx);

    static bool GetOrAllocateSegment(bool allocateIfNotExist,
                                     uint64_t offset,
                                     MDSClient* mdsClient,
                                     MetaCache* metaCache,
                                     const FInfo* fileInfo,
                                     ChunkIndex chunkidx);

    static int SplitForNormal(IOTracker* iotracker, MetaCache* metaCache,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fileInfo);

    static int SplitForStripe(IOTracker* iotracker, MetaCache* metaCache,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fileInfo);

 private:
    // IO拆分模块所使用的配置信息
    static IOSplitOption iosplitopt_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_SPLITOR_H_
