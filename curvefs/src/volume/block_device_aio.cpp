/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday Jul 07 14:59:19 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/block_device_aio.h"

#include <atomic>
#include <cstring>
#include <mutex>

#include "absl/memory/memory.h"
#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "src/client/libcurve_file.h"
#include "src/common/fast_align.h"

namespace curvefs {
namespace volume {

using ::curve::common::align_down;
using ::curve::common::align_up;
using ::curve::common::is_aligned;

namespace {

const char *ToString(LIBCURVE_OP op) {
    switch (op) {
    case LIBCURVE_OP_READ:
        return "Read";
    case LIBCURVE_OP_WRITE:
        return "Write";
    case LIBCURVE_OP_DISCARD:
        return "Discard";
    default:
        return "Unknown";
    }
}

std::ostream &operator<<(std::ostream &os, CurveAioContext *aio) {
    os << "[off: " << aio->offset << ", len: " << aio->length
       << ", ret: " << aio->ret << ", type: " << ToString(aio->op) << "]";

    return os;
}

void AioReadCallBack(CurveAioContext *aio) {
    AioRead *read = reinterpret_cast<AioRead *>(reinterpret_cast<char *>(aio) -
                                                offsetof(AioRead, aio));

    {
        std::lock_guard<std::mutex> lock(read->mtx);
        read->done = true;
    }
    read->cond.notify_one();
}

void AioWriteCallBack(CurveAioContext *aio) {
    AioWrite *write = reinterpret_cast<AioWrite *>(
        reinterpret_cast<char *>(aio) - offsetof(AioWrite, aio));

    {
        std::lock_guard<std::mutex> lock(write->mtx);
        write->done = true;
    }
    write->cond.notify_one();
}

void AioWritePaddingReadCallBack(CurveAioContext *aio) {
    AioWrite::PaddingRead *padding = reinterpret_cast<AioWrite::PaddingRead *>(
        reinterpret_cast<char *>(aio) - offsetof(AioWrite::PaddingRead, aio));

    padding->base->OnPaddingReadComplete(aio);
}

}  // namespace

AioRead::AioRead(off_t offset, size_t length, char *data, FileClient *dev,
                 int fd)
    : aio(), offset(offset), length(length), data(data), dev(dev), fd(fd) {}

void AioRead::Issue() {
    if (is_aligned(offset, IO_ALIGNED_BLOCK_SIZE) &&
        is_aligned(length, IO_ALIGNED_BLOCK_SIZE)) {
        aio.ret = -1;
        aio.op = LIBCURVE_OP_READ;
        aio.cb = AioReadCallBack;
        aio.offset = offset;
        aio.length = length;
        aio.buf = data;

        int ret = dev->AioRead(fd, &aio);
        if (ret < 0) {
            LOG(ERROR) << "Failed to issue aio read: " << &aio;
            done = true;
        }

        return;
    }

    // unaligned read request
    padding = absl::make_unique<Padding>();
    auto alignedOff = align_down(offset, IO_ALIGNED_BLOCK_SIZE);
    auto alignedEnd = align_up(offset + length, IO_ALIGNED_BLOCK_SIZE);

    padding->offset = alignedOff;
    padding->length = alignedEnd - alignedOff;
    padding->data.reset(new char[padding->length]);

    aio.ret = -1;
    aio.op = LIBCURVE_OP_READ;
    aio.cb = AioReadCallBack;
    aio.offset = padding->offset;
    aio.length = padding->length;
    aio.buf = padding->data.get();

    int ret = dev->AioRead(fd, &aio);
    if (ret < 0) {
        LOG(ERROR) << "Failed to issue aio read: " << &aio;
        done = true;
    }
}

ssize_t AioRead::Wait() {
    std::unique_lock<std::mutex> lock(mtx);
    cond.wait(lock, [this]() { return done; });

    if (static_cast<ssize_t>(aio.ret) != static_cast<ssize_t>(aio.length)) {
        LOG(ERROR) << "AioRead error: " << &aio;
        return -1;
    }

    if (padding != nullptr) {
        std::memcpy(data, padding->data.get() + (offset - padding->offset),
                    length);
    }

    return length;
}

AioWrite::AioWrite(off_t offset, size_t length, const char *data,
                   FileClient *dev, int fd)
    : offset(offset), length(length), data(data), dev(dev), fd(fd) {}

void AioWrite::Issue() {
    if (is_aligned(offset, IO_ALIGNED_BLOCK_SIZE) &&
        is_aligned(length, IO_ALIGNED_BLOCK_SIZE)) {
        aio.ret = -1;
        aio.op = LIBCURVE_OP_WRITE;
        aio.cb = AioWriteCallBack;
        aio.offset = offset;
        aio.length = length;
        aio.buf = const_cast<char *>(data);

        int ret = dev->AioWrite(fd, &aio);
        if (ret < 0) {
            LOG(ERROR) << "Failed to issue aio write: " << &aio;
            done = true;
        }

        return;
    }

    aux = absl::make_unique<PaddingAux>();

    aux->alignedOffset = align_down(offset, IO_ALIGNED_BLOCK_SIZE);
    auto alignedEnd = align_up(offset + length, IO_ALIGNED_BLOCK_SIZE);

    aux->shift = offset - aux->alignedOffset;
    aux->alignedLength = alignedEnd - aux->alignedOffset;

    aux->paddingData.reset(new char[aux->alignedLength]);

    int idx = 0;
    bool paddingStart = false;
    auto lastPaddingEnd = aux->alignedOffset;
    if (offset != aux->alignedOffset) {
        aux->paddingReads[idx].offset = aux->alignedOffset;
        aux->paddingReads[idx].length = IO_ALIGNED_BLOCK_SIZE;
        aux->paddingReads[idx].data = aux->paddingData.get();
        aux->paddingReads[idx].base = this;
        lastPaddingEnd = aux->alignedOffset + IO_ALIGNED_BLOCK_SIZE;
        paddingStart = true;
        ++idx;
    }

    if (static_cast<off_t>(offset + length) > lastPaddingEnd &&
        offset + length != alignedEnd) {
        off_t start = alignedEnd - IO_ALIGNED_BLOCK_SIZE;
        if (paddingStart && start == lastPaddingEnd) {
            aux->paddingReads[idx - 1].length += IO_ALIGNED_BLOCK_SIZE;
        } else {
            aux->paddingReads[idx].offset = start;
            aux->paddingReads[idx].length = IO_ALIGNED_BLOCK_SIZE;
            aux->paddingReads[idx].data =
                aux->paddingData.get() +
                (aux->alignedLength - IO_ALIGNED_BLOCK_SIZE);
            aux->paddingReads[idx].base = this;
            ++idx;
        }
    }

    assert(idx >= 1 && idx <= 2);
    aux->npadding.store(idx, std::memory_order_release);

    for (int i = 0; i < idx; ++i) {
        auto &pad = aux->paddingReads[i];

        pad.aio.ret = -1;
        pad.aio.op = LIBCURVE_OP_READ;
        pad.aio.cb = AioWritePaddingReadCallBack;
        pad.aio.buf = pad.data;
        pad.aio.offset = pad.offset;
        pad.aio.length = pad.length;

        int ret = dev->AioRead(fd, &pad.aio);
        if (ret < 0) {
            OnPaddingReadComplete(&pad.aio);
        }
    }
}

ssize_t AioWrite::Wait() {
    std::unique_lock<std::mutex> lock(mtx);
    cond.wait(lock, [this]() { return done; });

    if (static_cast<ssize_t>(aio.ret) != static_cast<ssize_t>(aio.length) ||
        (aux != nullptr && aux->error.load(std::memory_order_acquire))) {
        LOG(ERROR) << "AioWrite error: " << &aio;
        return -1;
    }

    return length;
}

void AioWrite::OnPaddingReadComplete(CurveAioContext *read) {
    if (static_cast<ssize_t>(read->ret) != static_cast<ssize_t>(read->length)) {
        LOG(ERROR) << "AioRead error: " << read;
        aux->error.store(true, std::memory_order_release);
    }

    if (1 != aux->npadding.fetch_sub(1, std::memory_order_acq_rel)) {
        return;
    }

    // padding read error
    if (aux->error.load(std::memory_order_acquire)) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            done = true;
        }
        cond.notify_one();
        return;
    }

    // copy origin write data
    std::memcpy(aux->paddingData.get() + aux->shift, data, length);

    aio.op = LIBCURVE_OP_WRITE;
    aio.buf = aux->paddingData.get();
    aio.offset = aux->alignedOffset;
    aio.length = aux->alignedLength;
    aio.cb = AioWriteCallBack;
    aio.ret = -1;

    int ret = dev->AioWrite(fd, &aio);
    if (ret < 0) {
        LOG(ERROR) << "Failed to issue aio write: " << &aio;
        {
            std::lock_guard<std::mutex> lock(mtx);
            done = true;
        }
        cond.notify_one();
    }
}

}  // namespace volume
}  // namespace curvefs
