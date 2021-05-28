/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCK_DEVICE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_BLOCK_DEVICE_CLIENT_H_

#include <memory>

#include "src/client/client_common.h"
#include "src/client/libcurve_file.h"
#include "curvefs/src/client/config.h"
#include "curvefs/src/client/error_code.h"

using ::curve::client::FileClient;

namespace curvefs {
namespace client {

class BlockDeviceClient {
 public:
    BlockDeviceClient() {}
    virtual ~BlockDeviceClient() {}

    /**
     * @brief Initailize client
     *
     * @return error code
     */
    virtual CURVEFS_ERROR Init(const BlockDeviceClientOptions &options) = 0;

    /**
     * @brief Uninitailize client
     *
     * @return error code
     */
    virtual CURVEFS_ERROR UnInit() = 0;

    /**
     * @brief read
     *
     * @param buf read buffer
     * @param offset read start offset
     * @param length read length
     *
     * @return error code
     */
    virtual CURVEFS_ERROR Read(char* buf, off_t offset, size_t length) = 0;


    /**
     * @brief write
     *
     * @param fd file descriptor
     * @param buf write buffer
     * @param offset write start offset
     * @param length write length
     *
     * @return error code
     */
    virtual CURVEFS_ERROR Write(
        const char* buf, off_t offset, size_t length) = 0;
};

class BlockDeviceClientImpl : public BlockDeviceClient {
 public:
    explicit BlockDeviceClientImpl(std::shared_ptr<FileClient> fileClient) :
        fileClient_(fileClient) {}
    virtual ~BlockDeviceClientImpl() {}

    CURVEFS_ERROR Init(const BlockDeviceClientOptions &options) override;

    CURVEFS_ERROR UnInit() override;

    CURVEFS_ERROR Read(char* buf, off_t offset, size_t length) override;

    CURVEFS_ERROR Write(const char* buf, off_t offset, size_t length) override;

 private:
    std::shared_ptr<FileClient> fileClient_;
    bool isOpen_;
    uint64_t fd_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_DEVICE_CLIENT_H_
