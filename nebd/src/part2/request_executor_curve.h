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
     * @brief 解析fileName
     *  一般格式:
     *    qemu "cbd:pool1//cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_:/etc/curve/client.conf" //NOLINT
     *    nbd  "cbd:pool1//cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_"  // NOLINT
     * @param[in] fileName
     * @return 解析出的字符串: "/cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_" //NOLINT
     */
    static std::string Parse(const std::string& fileName);
};

class CurveRequestExecutor : public NebdRequestExecutor {
 public:
    static CurveRequestExecutor& GetInstance() {
        static CurveRequestExecutor executor;
        return executor;
    }
    ~CurveRequestExecutor() {}
    void Init(const std::shared_ptr<CurveClient> &client);
    std::shared_ptr<NebdFileInstance> Open(const std::string& filename) override;  // NOLINT
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
     * @brief 构造函数
     */
    CurveRequestExecutor() {}

    /**
     * @brief 从NebdFileInstance中解析出curve_client需要的fd
     * @param[in] fd NebdFileInstance类型
     * @return 返回curve_client中文件的fd, 如果小于0，表示解析结果错误
     */
    int GetCurveFdFromNebdFileInstance(NebdFileInstance* fd);

    /**
     * @brief 从NebdFileInstance中解析出curbe_client需要的filename
     * @param[in] fd NebdFileInstance类型
     * @return 返回curve_client中的filename, 如果为空，表示解析出错
     */
    std::string GetFileNameFromNebdFileInstance(NebdFileInstance* fd);

    /**
     * @brief 将NebdServerAioContext类型转换为CurveAioContext类型
     * @param[in] nebdCtx NebdServerAioContext类型
     * @param[out] curveCtx CurveAioContext类型
     * @return -1转换失败，0转换成功
     */
    int FromNebdCtxToCurveCtx(
        NebdServerAioContext *nebdCtx, CurveAioContext *curveCtx);

    /**
     * @brief 将LIBAIO_OP类型转换为curve_client中LIBCURVE_OP类型
     * @param[in] op LIBAIO_OP类型
     * @param[out] out LIBCURVE_OP类型
     * @return -1转换失败，0转换成功
     */
     int FromNebdOpToCurveOp(LIBAIO_OP op, LIBCURVE_OP *out);

 private:
    std::shared_ptr<::curve::client::CurveClient> client_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_REQUEST_EXECUTOR_CURVE_H_
