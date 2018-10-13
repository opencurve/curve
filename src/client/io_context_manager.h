/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:15:27 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_IOCONTEXT_MANAGER_H
#define CURVE_IOCONTEXT_MANAGER_H

#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/context_slab.h"
#include "src/client/metacache.h"
#include "src/client/request_scheduler.h"

namespace curve {
namespace client {

class Session;
class CURVE_CACHELINE_ALIGNMENT IOContextManager {
 public:
    explicit IOContextManager(MetaCache* mc,
                RequestScheduler* scheduler = nullptr);
    ~IOContextManager();
    IOContextManager(const IOContextManager& other) = delete;
    IOContextManager& operator=(const IOContextManager& other) = delete;
    IOContextManager(const IOContextManager&& other) = delete;
    IOContextManager& operator=(const IOContextManager&& other) = delete;

    int Read(char* buf, off_t offset, size_t length);
    int Write(const char* buf, off_t offset, size_t length);
    void AioRead(CurveAioContext* aioctx);
    void AioWrite(CurveAioContext* aioctx);

    bool Initialize();
    void UnInitialize();

 private:
    MetaCache*  mc_;
    RequestScheduler* scheduler_;
    IOContextSlab*  iocontextslab_;
    RequestContextSlab*  requestContextslab_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_IOCONTEXT_MANAGER_H
