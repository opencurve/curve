/*
 * Project: curve
 * File Created: Monday, 17th September 2018 3:22:06 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_IOSPLIT_CONTEXT_H
#define CURVE_LIBCURVE_IOSPLIT_CONTEXT_H

#include <set>
#include <list>
#include <atomic>

#include "src/client/request_scheduler.h"
#include "include/curve_compiler_specific.h"
#include "src/client/request_context.h"
#include "src/client/context_slab.h"
#include "src/client/metacache.h"
#include "src/client/io_condition_varaiable.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {

class CURVE_CACHELINE_ALIGNMENT IOContext {
 public:
    explicit IOContext(IOContextSlab* iocontextslab);
    ~IOContext();

    void StartRead(CurveAioContext* aioctx,
                    MetaCache* mc,
                    RequestContextSlab* reqslab,
                    char* buf,
                    off_t offset,
                    size_t length);
    void StartWrite(CurveAioContext* aioctx,
                    MetaCache* mc,
                    RequestContextSlab* reqslab,
                    const char* buf,
                    off_t offset,
                    size_t length);

    void SetScheduler(RequestScheduler*);
    int Wait();
    void Reset();
    void Done();
    bool IsBusy();
    void RecycleSelf();
    /* if a piece of request failed, we just retry that piece*/
    void HandleResponse(RequestContext* reqctx);

    /*io type*/
    OpType  type_;

 private:
    /*io context status*/
    std::atomic<bool>  isReturned_;

    /*io context busy or not*/
    std::atomic<bool>  isBusy_;

    /*io content*/
    mutable const char*   data_;
    off_t   offset_;
    size_t  length_;

    /*io helper*/
    IOCond  iocond_;

    /*io result*/
    std::atomic<bool> success_;
    int errorcode_;

    /*io context*/
    CurveAioContext* aioctx_;

    /* result helper */
    std::atomic<uint32_t> donecount_;
    uint32_t reqcount_;

    /*context helper*/
    IOContextSlab* iocontextslab_;
    std::list<RequestContext*>   reqlist_;

    /* scheduler */
    RequestScheduler* scheduler_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_IOSPLIT_CONTEXT_H
