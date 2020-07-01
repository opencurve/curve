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
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_CORE_H_
#define SRC_CHUNKSERVER_CLONE_CORE_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <memory>

#include "proto/chunk.pb.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/datastore/define.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using curve::chunkserver::CSChunkInfo;
using common::TimeUtility;

class ReadChunkRequest;
class PasteChunkInternalRequest;
class CloneCore;

class DownloadClosure : public Closure {
 public:
    DownloadClosure(std::shared_ptr<ReadChunkRequest> readRequest,
                    std::shared_ptr<CloneCore> cloneCore,
                    AsyncDownloadContext* downloadCtx,
                    Closure *done);

    void Run();

    void SetFailed() {
        isFailed_ = true;
    }

    AsyncDownloadContext* GetDownloadContext() {
        return downloadCtx_;
    }

 protected:
    // 下载是否出错出错
    bool isFailed_;
    // 请求开始的时间
    uint64_t beginTime_;
    // 下载请求上下文信息
    AsyncDownloadContext* downloadCtx_;
    // clone core对象
    std::shared_ptr<CloneCore> cloneCore_;
    // read chunk请求对象
    std::shared_ptr<ReadChunkRequest> readRequest_;
    // DownloadClosure生命周期结束后需要执行的回调
    Closure* done_;
};

class CloneClosure : public Closure {
 public:
    CloneClosure() : request_(nullptr)
                   , response_(nullptr)
                   , userResponse_(nullptr)
                   , done_(nullptr) {}

    void Run();
    void SetClosure(Closure *done) {
        done_ = done;
    }
    void SetRequest(Message* request) {
        request_ = dynamic_cast<ChunkRequest *>(request);
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetUserResponse(Message* response) {
        userResponse_ = dynamic_cast<ChunkResponse *>(response);
    }

 private:
    // paste chunk的请求结构体
    ChunkRequest        *request_;
    // paste chunk的响应结构体
    ChunkResponse       *response_;
    // 真正要返回给用户的响应结构体
    ChunkResponse       *userResponse_;
    // CloneClosure生命周期结束后需要执行的回调
    Closure             *done_;
};

class CloneCore : public std::enable_shared_from_this<CloneCore> {
    friend class DownloadClosure;
 public:
    CloneCore(uint32_t sliceSize, bool enablePaste,
              std::shared_ptr<OriginCopyer> copyer)
        : sliceSize_(sliceSize)
        , enablePaste_(enablePaste)
        , copyer_(copyer) {}
    virtual ~CloneCore() {}

    /**
     * 处理读请求的逻辑
     * @param readRequest[in]:读请求信息
     * @param done[in]:任务完成后要执行的closure
     * @return: 成功返回0，失败返回-1
     */
    int HandleReadRequest(std::shared_ptr<ReadChunkRequest> readRequest,
                          Closure* done);

 protected:
    /**
     * 本地chunk文件存在情况下，按照本地记录的clone和bitmap信息进行数据读取
     * 会涉及读取远程文件结合本地文件进行merge返回结果
     * @param[in/out] readRequest: 用户请求&响应上下文
     * @param[in] chunkInfo: 对应本地的chunkinfo
     * @return 成功返回0，失败返回负数
     */
    int CloneReadByLocalInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        const CSChunkInfo &chunkInfo, Closure* done);

    /**
     * 本地chunk文件不存在情况下，按照用户请求上下文中带的clonesource信息进行数据读取
     * 不涉及merge本地结果
     * @param[in/out] readRequest: 用户请求&响应上下文
     */
    void CloneReadByRequestInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        Closure* done);

    /**
     * 从本地chunk中读取请求的区域，然后设置response
     * @param readRequest: 用户的ReadRequest
     * @return: 成功返回0，失败返回-1
     */
    int ReadChunk(std::shared_ptr<ReadChunkRequest> readRequest);

    /**
     * 设置read chunk类型的response，包括返回的数据和其他返回参数
     * 从本地chunk中读取已被写过的区域，未写过的区域从克隆下来的数据中获取
     * 然后将数据在内存中merge
     * @param readRequest: 用户的ReadRequest
     * @param cloneData: 从源端拷贝下来的数据，数据起始偏移同请求中的偏移
     * @return: 成功返回0，失败返回-1
     */
    int SetReadChunkResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                             const butil::IOBuf* cloneData);

    // 从本地chunk中读取已经写过的区域合并到clone data中
    int ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                      const CSChunkInfo& chunkInfo,
                      const butil::IOBuf* cloneData,
                      char* chunkData);

    /**
     * 将从源端下载下来的数据paste到本地chunk文件中
     * @param readRequest: 用户的ReadRequest
     * @param cloneData: 从源端下载的数据
     * @param offset: 下载的数据在chunk文件中的偏移
     * @param cloneDataSize: 下载的数据长度
     * @param done:任务完成后要执行的closure
     */
    void PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                        const butil::IOBuf* cloneData,
                        off_t offset,
                        size_t cloneDataSize,
                        Closure* done);

    inline void SetResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                            CHUNK_OP_STATUS status);

 private:
    // 每次拷贝的slice的大小
    uint32_t sliceSize_;
    // 判断read chunk类型的请求是否需要paste, true需要paste，false表示不需要
    bool enablePaste_;
    // 负责从源端下载数据
    std::shared_ptr<OriginCopyer> copyer_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_CORE_H_
