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
#include <cstring>

#include "include/client/libcurve.h"
#include "nebd/src/part1/libnebd.h"

namespace curve {
namespace nbd {

union AioRequestContext {
    CurveAioContext curveAioCtx;
    NebdClientAioContext nebdAioCtx;
};

// 封装nebd相关接口
class ImageInstance {
 public:
    explicit ImageInstance(const std::string& imageName)
        : fd_(-1), imageName_(imageName) {}

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
     * @return 请求是否发起成功
     */
    virtual bool AioRead(AioRequestContext* context) = 0;

    /**
     * @brief 异步写请求
     * @param context 写请求信息
     * @return 请求是否发起成功
     */
    virtual bool AioWrite(AioRequestContext* context) = 0;

    /**
     * @brief trim请求
     * @param context trim请求信息
     * @return 请求是否发起成功
     */
    virtual bool Trim(AioRequestContext* context) = 0;

    /**
     * @brief flush请求
     * @param context flush请求信息
     * @return 请求是否发起成功
     */
    virtual bool Flush(AioRequestContext* context) = 0;

    /**
     * @brief 获取文件大小
     * @return 获取成功返回文件大小（正值）
     *         获取失败返回错误码（负值）
     */
    virtual int64_t GetImageSize() = 0;

    const std::string& GetFileName() const {
        return imageName_;
    }

 protected:
    // 文件描述符
    int fd_;

    // 卷名
    std::string imageName_;
};

using ImagePtr = std::shared_ptr<ImageInstance>;

class NebdImageInstance : public ImageInstance {
 public:
    explicit NebdImageInstance(const std::string& imageName)
        : ImageInstance(imageName) {
        if (imageName_.size() > 0 && imageName_[0] == '/') {
            imageName_ = "cbd:pool/" + imageName_;
        }
    }

    virtual ~NebdImageInstance();

    bool Open() override;

    void Close() override;

    bool AioRead(AioRequestContext* context) override;

    bool AioWrite(AioRequestContext* context) override;

    bool Trim(AioRequestContext* context) override;

    bool Flush(AioRequestContext* context) override;

    int64_t GetImageSize() override;
};

class CurveImageInstance : public ImageInstance {
 public:
    explicit CurveImageInstance(const std::string& imageName)
        : ImageInstance(imageName) {}

    virtual ~CurveImageInstance();

    bool Open() override;

    void Close() override;

    bool AioRead(AioRequestContext* context) override;

    bool AioWrite(AioRequestContext* context) override;

    bool Flush(AioRequestContext* context) override;

    bool Trim(AioRequestContext* context) override;

    int64_t GetImageSize() override;

 private:
    curve::client::CurveClient client_;
};

}  // namespace nbd
}  // namespace curve

#endif  // NBD_SRC_IMAGEINSTANCE_H_
