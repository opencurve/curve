/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: lixiaocui
 */

#ifndef NEBD_SRC_PART2_REQUEST_EXECUTOR_CURVE_H_
#define NEBD_SRC_PART2_REQUEST_EXECUTOR_CURVE_H_

#include <string>
#include <memory>
#include <utility>
#include "nebd/src/part2/request_executor.h"
#include "nebd/src/part2/define.h"
#include "include/client/libcurve.h"

namespace nebd {
namespace server {

using ::curve::client::CurveClient;

class CurveFileInstance : public NebdFileInstance {
 public:
    CurveFileInstance() {}
    ~CurveFileInstance() {}

    int fd = -1;
    std::string fileName;
};

class CurveAioCombineContext {
 public:
    NebdServerAioContext* nebdCtx;
    CurveAioContext curveCtx;
};
void CurveAioCallback(struct CurveAioContext* curveCtx);

class FileNameParser {
 public:
    /**
     * @brief parsing fileName
     * General format:
     *    qemu "cbd:pool1//cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_:/etc/curve/client.conf" //NOLINT
     *    nbd  "cbd:pool1//cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_"  // NOLINT
     * @param[in] fileName
     * @return Parsing Result
     *  qemu "/cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_", "/etc/curve/client.conf" //NOLINT
     *  nbd  "/cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_", "" //NOLINT
     */
    static std::pair<std::string, std::string>
            Parse(const std::string& fileName);
};

class CurveRequestExecutor : public NebdRequestExecutor {
 public:
    static CurveRequestExecutor& GetInstance() {
        static CurveRequestExecutor executor;
        return executor;
    }
    ~CurveRequestExecutor() {}
    void Init(const std::shared_ptr<CurveClient> &client);
    std::shared_ptr<NebdFileInstance> Open(const std::string& filename,
                                           const OpenFlags* openflags) override;
    std::shared_ptr<NebdFileInstance> Reopen(
        const std::string& filename, const ExtendAttribute& xattr) override;
    int Close(NebdFileInstance* fd) override;
    int Extend(NebdFileInstance* fd, int64_t newsize) override;
    int GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) override;
    int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int InvalidCache(NebdFileInstance* fd) override;

 private:
    /**
     * @brief constructor
     */
    CurveRequestExecutor() {}

    /**
     * @brief Parse curve from NebdFileInstance_ Fd required by client
     * @param[in] fd NebdFileInstance type
     * @return returns the curve_ If the fd of the file in the client is less than 0, it indicates that the parsing result is incorrect
     */
    int GetCurveFdFromNebdFileInstance(NebdFileInstance* fd);

    /**
     * @brief Parsing curbe from NebdFileInstance_ The filename required by the client
     * @param[in] fd NebdFileInstance type
     * @return returns the curve_ The filename in the client, if empty, indicates an error in parsing
     */
    std::string GetFileNameFromNebdFileInstance(NebdFileInstance* fd);

    /**
     * @brief Convert NebdServerAioContext type to CurveAioContext type
     * @param[in] nebdCtx NebdServerAioContext type
     * @param[out] curveCtx CurveAioContext type
     * @return -1 conversion failed, 0 conversion succeeded
     */
    int FromNebdCtxToCurveCtx(
        NebdServerAioContext *nebdCtx, CurveAioContext *curveCtx);

    /**
     * @brief will LIBAIO_ OP type conversion to curve_ LIBCURVE in client_ OP type
     * @param[in] op LIBAIO_ OP type
     * @param[out] out LIBCURVE_ OP type
     * @return -1 conversion failed, 0 conversion succeeded
     */
     int FromNebdOpToCurveOp(LIBAIO_OP op, LIBCURVE_OP *out);

 private:
    std::shared_ptr<::curve::client::CurveClient> client_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_REQUEST_EXECUTOR_CURVE_H_
