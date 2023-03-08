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
 * Project: curve
 * Date: Thu Apr 23 09:50:18 CST 2020
 * Author: wuhanqing
 */

#ifndef NBD_SRC_IMAGEINSTANCE_H_
#define NBD_SRC_IMAGEINSTANCE_H_

#include <memory>
#include <string>

#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "nbd/src/define.h"
#include "nebd/src/part1/libnebd.h"

namespace curve {
namespace nbd {

union AioContext {
    CurveAioContext curve;
    NebdClientAioContext nebd;
};

class ImageInstance {
 public:
    ImageInstance(const std::string& imageName, const NBDConfig* config)
        : fd_(-1), imageName_(imageName), config_(config) {}

    ImageInstance(const ImageInstance&) = delete;
    ImageInstance& operator=(const ImageInstance&) = delete;

    virtual ~ImageInstance() = default;

    /**
     * @brief 打开卷
     * @return 打开成功返回true
     *         打开失败返回false
     */
    virtual bool Open() = 0;

    /**
     * @brief 关闭卷
     */
    virtual void Close() = 0;

    /**
     * @brief 异步读请求
     * @param context 读请求信息
     */
    virtual bool AioRead(AioContext* context) = 0;

    /**
     * @brief 异步写请求
     * @param context 写请求信息
     */
    virtual bool AioWrite(AioContext* context) = 0;

    /**
     * @brief trim请求
     * @param context trim请求信息
     */
    virtual bool Trim(AioContext* context) = 0;

    /**
     * @brief flush请求
     * @param context flush请求信息
     */
    virtual bool Flush(AioContext* context) = 0;

    /**
     * @brief 获取文件大小
     * @return 获取成功返回文件大小（正值）
     *         获取失败返回错误码（负值）
     */
    virtual int64_t GetImageSize() = 0;

 protected:
    int fd_;
    std::string imageName_;

    const NBDConfig* config_;
};

using ImagePtr = std::shared_ptr<ImageInstance>;

class NebdImage : public ImageInstance {
 public:
    NebdImage(const std::string& imageName, const NBDConfig* config)
        : ImageInstance{imageName, config} {}

    ~NebdImage() override;

    bool Open() override;

    void Close() override;

    bool AioRead(AioContext* context) override;

    bool AioWrite(AioContext* context) override;

    bool Trim(AioContext* context) override;

    bool Flush(AioContext* context) override;

    int64_t GetImageSize() override;
};

class CurveImage : public ImageInstance {
 public:
    CurveImage(const std::string& imageName, const NBDConfig* config)
        : ImageInstance{imageName, config} {}

    ~CurveImage() override;

    bool Open() override;

    void Close() override;

    bool AioRead(AioContext* context) override;

    bool AioWrite(AioContext* context) override;

    bool Flush(AioContext* context) override;

    bool Trim(AioContext* context) override;

    int64_t GetImageSize() override;

    // Strip prefix `cbd:pool/` from name
    static std::string TransformImageName(const std::string& name);

 private:
    curve::client::CurveClient client_;
    std::string actualImageName_;
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_SRC_IMAGEINSTANCE_H_
