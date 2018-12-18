/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:19:43 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_REQUEST_CONTEXT_H
#define CURVE_LIBCURVE_REQUEST_CONTEXT_H

#include <atomic>

#include "src/client/client_common.h"

namespace curve {
namespace client {

enum class OpType {
    READ,
    WRITE,
    UNKNOWN
};
class RequestClosure;
class RequestContext {
 public:
    explicit RequestContext(RequestContextSlab* reqctxslab);
    ~RequestContext();

    void Reset();
    void RecyleSelf();

    mutable const char*  data_;
    off_t               offset_;
    OpType              optype_;
    /**
     * at chunkserver we treat fail if not read the length as the
     * rpc assigned. so if the rpc returned length is not equal to
     * rawlength_, we think the read is failed.
     */
    size_t          rawlength_;
    ChunkID         chunkid_;
    CopysetID       copysetid_;
    LogicPoolID     logicpoolid_;
    RequestClosure* done_;

    uint64_t    appliedindex_;

 private:
    RequestContextSlab* reqctxslab_;
};
}  // namespace client
}  // namespace curve
#endif  // !CURVE_LIBCURVE_REQUEST_CONTEXT_H
