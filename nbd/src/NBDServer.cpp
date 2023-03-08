/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/**
 * Project : curve
 * Date : Wed Apr 22 17:04:51 CST 2020
 * Author: wuhanqing
 */

/*
 * rbd-nbd - RBD in userspace
 *
 * Copyright (C) 2015 - 2016 Kylin Corporation
 *
 * Author: Yunchuan Wen <yunchuan.wen@kylin-cloud.com>
 *         Li Wang <li.wang@kylin-cloud.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "nbd/src/NBDServer.h"

#include <signal.h>
#include <glog/logging.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "nbd/src/util.h"

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

static void NEBDAioCallback(struct NebdClientAioContext* aioCtx) {
    IOContext* ctx = reinterpret_cast<IOContext*>(
            reinterpret_cast<char*>(aioCtx) - offsetof(IOContext, aio));

    if (aioCtx->ret != 0) {
        ctx->reply.error = htonl(-aioCtx->ret);
    } else {
        ctx->reply.error = htonl(0);
    }

    ctx->server->OnRequestFinish(ctx);
}

static void CurveAioCallback(struct CurveAioContext* aioCtx) {
    IOContext* ctx = reinterpret_cast<IOContext*>(
            reinterpret_cast<char*>(aioCtx) - offsetof(IOContext, aio));

    if (aioCtx->ret < 0) {
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
            if (config_->use_curvesdk) {
                pctx->aio.curve.ret = -1;
            } else {
                pctx->aio.nebd.ret = -1;
            }

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
    LOG(INFO) << "WaitClean, current pending requests: "
              << pendingRequestCounts_;
    std::unique_lock<std::mutex> lk(requestMtx_);
    requestCond_.wait(lk, [this]() { return pendingRequestCounts_ == 0; });

    while (!finishedRequests_.empty()) {
        std::unique_ptr<IOContext> ctx(finishedRequests_.front());
        finishedRequests_.pop_front();
    }
}

bool NBDServer::StartAioRequest(IOContext* ctx) {
    SetUpAioRequest(ctx);

    switch (ctx->command) {
        case NBD_CMD_READ:
            return image_->AioRead(&ctx->aio);
        case NBD_CMD_WRITE:
            return image_->AioWrite(&ctx->aio);
        case NBD_CMD_FLUSH:
            return image_->Flush(&ctx->aio);
        case NBD_CMD_TRIM:
            return image_->Trim(&ctx->aio);
        default:
            LOG(ERROR) << "Invalid request type: " << *ctx;
            return false;
    }

    return false;
}

void NBDServer::SetUpAioRequest(IOContext* ctx) {
    const int command = ctx->command;

    if (!config_->use_curvesdk) {
        switch (command) {
            case NBD_CMD_READ:
                ctx->aio.nebd.op = LIBAIO_OP_READ;
                ctx->aio.nebd.cb = NEBDAioCallback;
                ctx->aio.nebd.offset = ctx->request.from;
                ctx->aio.nebd.length = ctx->request.len;
                ctx->aio.nebd.buf = ctx->data.get();
                break;
            case NBD_CMD_WRITE:
                ctx->aio.nebd.op = LIBAIO_OP_WRITE;
                ctx->aio.nebd.cb = NEBDAioCallback;
                ctx->aio.nebd.offset = ctx->request.from;
                ctx->aio.nebd.length = ctx->request.len;
                ctx->aio.nebd.buf = ctx->data.get();
                break;
            case NBD_CMD_FLUSH:
                ctx->aio.nebd.op = LIBAIO_OP_FLUSH;
                ctx->aio.nebd.cb = NEBDAioCallback;
                break;
            case NBD_CMD_TRIM:
                ctx->aio.nebd.op = LIBAIO_OP_DISCARD;
                ctx->aio.nebd.cb = NEBDAioCallback;
                ctx->aio.nebd.offset = ctx->request.from;
                ctx->aio.nebd.length = ctx->request.len;
                break;
            default:
                break;
        }
    } else {
        switch (command) {
            case NBD_CMD_READ:
                ctx->aio.curve.op = LIBCURVE_OP_READ;
                ctx->aio.curve.cb = CurveAioCallback;
                ctx->aio.curve.offset = ctx->request.from;
                ctx->aio.curve.length = ctx->request.len;
                ctx->aio.curve.buf = ctx->data.get();
                break;
            case NBD_CMD_WRITE:
                ctx->aio.curve.op = LIBCURVE_OP_WRITE;
                ctx->aio.curve.cb = CurveAioCallback;
                ctx->aio.curve.offset = ctx->request.from;
                ctx->aio.curve.length = ctx->request.len;
                ctx->aio.curve.buf = ctx->data.get();
                break;
            case NBD_CMD_FLUSH:
                ctx->aio.curve.op = LIBCURVE_OP_FLUSH;
                ctx->aio.curve.cb = CurveAioCallback;
                break;
            case NBD_CMD_TRIM:
                ctx->aio.curve.op = LIBCURVE_OP_DISCARD;
                ctx->aio.curve.cb = CurveAioCallback;
                ctx->aio.curve.offset = ctx->request.from;
                ctx->aio.curve.length = ctx->request.len;
                break;
            default:
                break;
        }
    }
}

}  // namespace nbd
}  // namespace curve
