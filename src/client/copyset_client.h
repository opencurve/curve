/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_COPYSETCLIENT_H
#define CURVE_CLIENT_COPYSETCLIENT_H

#include <google/protobuf/stubs/callback.h>
#include <gflags/gflags.h>

#include <cstdio>
#include <string>

#include "src/client/client_common.h"
#include "src/common/uncopyable.h"
#include "src/client/request_sender_manager.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

DECLARE_int32(client_chunk_op_retry_interval_us);
DECLARE_int32(client_chunk_op_max_retry);

using curve::common::Uncopyable;

class MetaCache;

class CopysetClient : public Uncopyable {
 public:
    CopysetClient() {}

    MetaCache *GetMetaCache() { return metaCache_; }
    RequestSenderManager *GetRequestSenderManager() {return senderManager_; }

    int Init(RequestSenderManager* senderManager, MetaCache* metaCache);
    int ReadChunk(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  off_t offset,
                  size_t length,
                  google::protobuf::Closure *done,
                  int retriedTimes = 0);
    int WriteChunk(LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   const char *buf,
                   off_t offset,
                   size_t length,
                   google::protobuf::Closure *done,
                   int retriedTimes = 0);


 private:
    MetaCache* metaCache_;
    RequestSenderManager* senderManager_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_COPYSETCLIENT_H
