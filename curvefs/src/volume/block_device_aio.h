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
 * Date: Thursday Jul 07 14:56:25 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_VOLUME_BLOCK_DEVICE_AIO_H_
#define CURVEFS_SRC_VOLUME_BLOCK_DEVICE_AIO_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "include/client/libcurve_define.h"

namespace curve {
namespace client {
class FileClient;
}  // namespace client
}  // namespace curve

namespace curvefs {
namespace volume {

using ::curve::client::FileClient;

struct AioRead {
    // aio context
    CurveAioContext aio;

    // original request context
    off_t offset;
    size_t length;
    char* data;

    // backend block device
    FileClient* dev;
    int fd;

    // synchronization
    bool done = false;
    std::mutex mtx;
    std::condition_variable cond;

    struct Padding {
        off_t offset;
        size_t length;
        std::unique_ptr<char[]> data;
    };

    // padding read request if necessary
    std::unique_ptr<Padding> padding;

    AioRead(off_t offset, size_t length, char* data, FileClient* dev, int fd);

    // Issue the read request.
    void Issue();

    // Wait until request is finished.
    // Return read bytes if succeeded, other return values mean an error
    // occurred.
    ssize_t Wait();
};

struct AioWrite {
    // aio context
    CurveAioContext aio;

    // original request context
    off_t offset;
    size_t length;
    const char* data;

    // backend block device
    FileClient* dev;
    int fd;

    // synchronization
    bool done = false;
    std::mutex mtx;
    std::condition_variable cond;

    // padding read request
    struct PaddingRead {
        CurveAioContext aio;

        off_t offset;
        size_t length;
        char* data;

        AioWrite* base;
    };

    struct PaddingAux {
        // one write request has at most 2 padding read request
        std::array<PaddingRead, 2> paddingReads;
        std::unique_ptr<char[]> paddingData;
        std::atomic<int> npadding;
        std::atomic<bool> error{false};
        size_t shift;
        off_t alignedOffset;
        size_t alignedLength;
    };

    std::unique_ptr<PaddingAux> aux;

    AioWrite(off_t offset,
             size_t length,
             const char* data,
             FileClient* dev,
             int fd);

    // Issue the write request.
    void Issue();

    // Wait until request is finished.
    // Return written bytes if succeeded, other return values mean an error
    // occurred.
    ssize_t Wait();

    void OnPaddingReadComplete(CurveAioContext* read);
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_BLOCK_DEVICE_AIO_H_
