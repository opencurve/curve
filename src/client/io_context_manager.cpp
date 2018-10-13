/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:15:38 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/client/io_context_manager.h"
#include "src/client/session.h"
#include "src/client/io_context.h"

DEFINE_int32(page_size, 4*1024, "size of crc page size");

namespace curve {
namespace client {

    IOContextManager::IOContextManager(MetaCache* mc,
                        RequestScheduler* scheduler) {
        mc_ = mc;
        scheduler_ = scheduler;
        iocontextslab_ = nullptr;
        requestContextslab_ = nullptr;
    }

    IOContextManager::~IOContextManager() {
    }

    bool IOContextManager::Initialize() {
        iocontextslab_ = new (std::nothrow) IOContextSlab();
        if (iocontextslab_ != nullptr) {
            if (!iocontextslab_->Initialize()) {
                delete iocontextslab_;
                iocontextslab_ = nullptr;
                LOG(ERROR) << "Create IOContextSlab failed!";
                return false;
            }
        }
        requestContextslab_ = new (std::nothrow) RequestContextSlab();
        if (requestContextslab_ != nullptr) {
            if (!requestContextslab_->Initialize()) {
                delete requestContextslab_;
                delete iocontextslab_;
                requestContextslab_ = nullptr;
                LOG(ERROR) << "Create RequestContextSlab failed!";
                return false;
            }
        }
        return true;
    }

    void IOContextManager::UnInitialize() {
        if (iocontextslab_ != nullptr) {
            iocontextslab_->UnInitialize();
            delete iocontextslab_;
        }
        iocontextslab_ = nullptr;

        if (requestContextslab_ != nullptr) {
            requestContextslab_->UnInitialize();
            delete requestContextslab_;
        }
        requestContextslab_ = nullptr;
    }

    int IOContextManager::Read(char* buf, off_t offset, size_t length) {
        IOContext* temp = iocontextslab_->Get();
        temp->SetScheduler(scheduler_);
        temp->StartRead(nullptr,
                        mc_,
                        requestContextslab_,
                        static_cast<char*>(buf),
                        offset,
                        length);
        return temp->Wait();
    }

    int IOContextManager::Write(const char* buf, off_t offset, size_t length) {
        IOContext* temp = iocontextslab_->Get();
        temp->SetScheduler(scheduler_);
        temp->StartWrite(nullptr,
                        mc_,
                        requestContextslab_,
                        static_cast<const char*>(buf),
                        offset,
                        length);
        return temp->Wait();
    }

    void IOContextManager::AioRead(CurveAioContext* aioctx) {
        IOContext* temp = iocontextslab_->Get();
        temp->SetScheduler(scheduler_);
        temp->StartRead(aioctx,
                        mc_,
                        requestContextslab_,
                        static_cast<char*>(aioctx->buf),
                        aioctx->offset,
                        aioctx->length);
    }

    void  IOContextManager::AioWrite(CurveAioContext* aioctx) {
        IOContext* temp = iocontextslab_->Get();
        temp->SetScheduler(scheduler_);
        temp->StartWrite(aioctx,
                        mc_,
                        requestContextslab_,
                        static_cast<const char*>(aioctx->buf),
                        aioctx->offset,
                        aioctx->length);
    }
}   // namespace client
}   // namespace curve
