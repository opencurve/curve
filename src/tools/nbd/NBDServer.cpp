/**
 * Project : curve
 * Date : Wed Apr 22 17:04:51 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#include "src/tools/nbd/NBDServer.h"

#include <signal.h>
#include <glog/logging.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "src/tools/nbd/util.h"

namespace curve {
namespace nbd {

#define REQUEST_TYPE_MASK 0x0000ffff

static std::ostream& operator<<(std::ostream& os, const IOContext& ctx) {
    os << "[" << std::hex
       << ntohll(*reinterpret_cast<uint64_t*>(
              const_cast<char*>(ctx.request.handle)));

    switch (ctx.command) {
        case NBD_CMD_WRITE:
            os << " WRITE ";
            break;
        case NBD_CMD_READ:
            os << " READ ";
            break;
        case NBD_CMD_FLUSH:
            os << " FLUSH ";
            break;
        case NBD_CMD_TRIM:
            os << " TRIM ";
            break;
        default:
            os << " UNKNOWN(" << ctx.command << ") ";
            break;
    }

    os << ctx.request.from << "~" << ctx.request.len << " " << std::dec
       << ntohl(ctx.reply.error) << "]";

    return os;
}

void NBDServer::NBDAioCallback(struct NebdClientAioContext* aioCtx) {
    IOContext* ctx = reinterpret_cast<IOContext*>(
        reinterpret_cast<char*>(aioCtx) - offsetof(IOContext, nebdAioCtx));

    if (aioCtx->ret != 0) {
        ctx->reply.error = htonl(-aioCtx->ret);
    } else {
        ctx->reply.error = htonl(0);
    }

    ctx->server->OnRequestFinish(ctx);
}

NBDServer::~NBDServer() {
    if (started_) {
        LOG(INFO) << "NBDServer going to quit";

        Shutdown();

        writerThread_.join();
        readerThread_.join();

        WaitClean();

        started_ = false;

        LOG(INFO) << "NBDServer quit";
    }
}

void NBDServer::Start() {
    if (started_) {
        return;
    }

    started_ = true;

    readerThread_ = std::thread(&NBDServer::ReaderFunc, this);
    writerThread_ = std::thread(&NBDServer::WriterFunc, this);

    return;
}

void NBDServer::WaitForDisconnect() {
    if (!started_) {
        return;
    }

    std::unique_lock<std::mutex> lk(disconnectMutex_);
    disconnectCond_.wait(lk);
}

void NBDServer::Shutdown() {
    LOG(INFO) << "going to shutdown, terminated " << terminated_;
    bool expected = false;

    if (terminated_.compare_exchange_strong(expected, true)) {
        shutdown(sock_, SHUT_RDWR);

        std::lock_guard<std::mutex> lk(requestMtx_);
        requestCond_.notify_all();
    }
}

void NBDServer::ReaderFunc() {
    ssize_t r = 0;
    bool disconnect = false;

    while (!terminated_) {
        std::unique_ptr<IOContext> ctx(new IOContext());
        ctx->server = this;

        r = safeIO_->ReadExact(sock_, &ctx->request, sizeof(ctx->request));
        if (r < 0) {
            LOG(ERROR) << "Failed to read nbd request header: "
                       << cpp_strerror(r);
            break;
        }

        if (ctx->request.magic != htonl(NBD_REQUEST_MAGIC)) {
            LOG(ERROR) << "Invalid nbd request magic" << std::hex
                       << ctx->request.magic;
            break;
        }

        ctx->request.from = ntohll(ctx->request.from);
        ctx->request.type = ntohl(ctx->request.type);
        ctx->request.len = ntohl(ctx->request.len);

        ctx->reply.magic = htonl(NBD_REPLY_MAGIC);
        memcpy(ctx->reply.handle, ctx->request.handle,
               sizeof(ctx->reply.handle));

        ctx->command = ctx->request.type & REQUEST_TYPE_MASK;

        switch (ctx->command) {
            case NBD_CMD_DISC:
                LOG(INFO) << "Receive DISC request";
                disconnect = true;
                break;
            case NBD_CMD_WRITE:
                ctx->data.reset(new char[ctx->request.len]);

                // 写请求，继续读取写入数据
                r = safeIO_->ReadExact(sock_, ctx->data.get(),
                                       ctx->request.len);
                if (r < 0) {
                    LOG(ERROR) << "Failed to read nbd request data "
                               << cpp_strerror(r);
                    disconnect = true;
                }
                break;
            case NBD_CMD_READ:
                ctx->data.reset(new char[ctx->request.len]);
                break;
        }

        if (disconnect) {
            break;
        }

        OnRequestStart();

        IOContext* pctx = ctx.get();
        bool ret = StartAioRequest(ctx.release());

        if (ret == false) {
            pctx->nebdAioCtx.ret = -1;
            OnRequestFinish(pctx);
            break;
        }
    }

    std::lock_guard<std::mutex> lk(disconnectMutex_);
    disconnectCond_.notify_all();

    LOG(INFO) << "ReaderFunc terminated!";

    Shutdown();
}

void NBDServer::WriterFunc() {
    signal(SIGPIPE, SIG_IGN);

    ssize_t r = 0;

    while (!terminated_) {
        std::unique_ptr<IOContext> ctx(WaitRequestFinish());

        if (ctx == nullptr) {
            LOG(INFO) << "No more requests, terminating";
            break;
        }

        r = safeIO_->Write(sock_, &ctx->reply, sizeof(struct nbd_reply));
        if (r < 0) {
            LOG(ERROR) << *ctx << ": failed to write reply header : "
                       << cpp_strerror(r);
            return;
        }

        if (ctx->command == NBD_CMD_READ && ctx->reply.error == htonl(0)) {
            r = safeIO_->Write(sock_, ctx->data.get(), ctx->request.len);
            if (r < 0) {
                LOG(ERROR) << *ctx << ": faield to write reply date : "
                           << cpp_strerror(r);
                return;
            }
        }
    }

    LOG(INFO) << "WriterFunc terminated!";
    Shutdown();
}

IOContext* NBDServer::WaitRequestFinish() {
    std::unique_lock<std::mutex> lk(requestMtx_);
    requestCond_.wait(
        lk, [this]() { return !finishedRequests_.empty() || terminated_; });

    if (finishedRequests_.empty()) {
        return nullptr;
    }

    IOContext* ctx = finishedRequests_.front();
    finishedRequests_.pop_front();

    return ctx;
}

void NBDServer::OnRequestStart() {
    std::lock_guard<std::mutex> lk(requestMtx_);
    ++pendingRequestCounts_;
}

void NBDServer::OnRequestFinish(IOContext* ctx) {
    std::lock_guard<std::mutex> lk(requestMtx_);

    --pendingRequestCounts_;

    finishedRequests_.push_back(ctx);
    requestCond_.notify_all();
}

void NBDServer::WaitClean() {
    LOG(INFO) << "WaitClean";
    std::unique_lock<std::mutex> lk(requestMtx_);
    requestCond_.wait(lk, [this]() { return pendingRequestCounts_ == 0; });

    while (!finishedRequests_.empty()) {
        std::unique_ptr<IOContext> ctx(finishedRequests_.front());
        finishedRequests_.pop_front();
    }
}

bool NBDServer::StartAioRequest(IOContext* ctx) {
    switch (ctx->command) {
        case NBD_CMD_WRITE:
            ctx->nebdAioCtx.offset = ctx->request.from;
            ctx->nebdAioCtx.length = ctx->request.len;
            ctx->nebdAioCtx.buf = ctx->data.get();
            ctx->nebdAioCtx.cb = NBDAioCallback;
            ctx->nebdAioCtx.op = LIBAIO_OP::LIBAIO_OP_WRITE;
            image_->AioWrite(&ctx->nebdAioCtx);
            return true;
        case NBD_CMD_READ:
            ctx->nebdAioCtx.offset = ctx->request.from;
            ctx->nebdAioCtx.length = ctx->request.len;
            ctx->nebdAioCtx.buf = ctx->data.get();
            ctx->nebdAioCtx.cb = NBDAioCallback;
            ctx->nebdAioCtx.op = LIBAIO_OP::LIBAIO_OP_READ;
            image_->AioRead(&ctx->nebdAioCtx);
            return true;
        case NBD_CMD_FLUSH:
            ctx->nebdAioCtx.op = LIBAIO_OP::LIBAIO_OP_FLUSH;
            ctx->nebdAioCtx.cb = NBDAioCallback;
            image_->Flush(&ctx->nebdAioCtx);
            return true;
        case NBD_CMD_TRIM:
            ctx->nebdAioCtx.offset = ctx->request.from;
            ctx->nebdAioCtx.length = ctx->request.len;
            ctx->nebdAioCtx.cb = NBDAioCallback;
            ctx->nebdAioCtx.op = LIBAIO_OP::LIBAIO_OP_DISCARD;
            image_->Trim(&ctx->nebdAioCtx);
            return true;
        default:
            LOG(ERROR) << "Invalid request type: " << *ctx;
            return false;
    }

    return false;
}

}  // namespace nbd
}  // namespace curve
