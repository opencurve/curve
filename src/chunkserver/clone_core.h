/*
 * Project: curve
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
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
#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/datastore/define.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using curve::chunkserver::CSChunkInfo;

class ReadChunkRequest;
class PasteChunkInternalRequest;

class CloneClosure : public Closure {
 public:
    CloneClosure() : request_(nullptr)
                   , done_(nullptr) {}

    void Run();
    void SetClosure(Closure *done) {
        done_ = done;
    }
    void SetRequest(Message* request) {
        request_ = dynamic_cast<ChunkRequest *>(request);
    }

 private:
    ChunkRequest        *request_;
    Closure             *done_;
};

class CloneCore {
 public:
    CloneCore(uint32_t sliceSize, std::shared_ptr<OriginCopyer> copyer)
        : sliceSize_(sliceSize)
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

 private:
    /**
     * 从本地chunk中读取已被写过的区域，未写过的区域从克隆下来的数据中获取
     * 将数据在内存中merge以后，将请求返回给用户
     * @param readRequest: 用户的ReadRequest
     * @param chunkInfo: 本地chunk的信息
     * @param cloneData: 从源端拷贝下来的数据，数据起始偏移同请求中的偏移
     * @param done:任务完成后要执行的closure
     * @return: 成功返回0，失败返回-1
     */
    int ReadMergeResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                          const CSChunkInfo& chunkInfo,
                          const char* cloneData,
                          Closure* done);

    /**
     * 将从源端下载下来的数据paste到本地chunk文件中
     * @param readRequest: 用户的ReadRequest
     * @param cloneData: 从源端下载的数据
     * @param offset: 下载的数据在chunk文件中的偏移
     * @param cloneDataSize: 下载的数据长度
     * @param done:任务完成后要执行的closure
     */
    void PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                        const char* cloneData,
                        off_t offset,
                        size_t cloneDataSize,
                        Closure* done);

    inline void SetResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                            CHUNK_OP_STATUS status);

 private:
    // 每次拷贝的slice的大小
    uint32_t sliceSize_;
    // 负责从源端下载数据
    std::shared_ptr<OriginCopyer> copyer_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_CORE_H_
