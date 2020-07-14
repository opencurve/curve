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

#ifndef SRC_TOOLS_NBD_IMAGEINSTANCE_H_
#define SRC_TOOLS_NBD_IMAGEINSTANCE_H_

#include <string>
#include <memory>
#include "src/tools/nbd/libnebd.h"

namespace curve {
namespace nbd {

// 封装nebd相关接口
class ImageInstance {
 public:
    explicit ImageInstance(const std::string& imageName)
        : fd_(-1), imageName_(imageName) {}

    virtual ~ImageInstance();

    /**
     * @brief 打开卷
     * @return 打开成功返回true
     *         打开失败返回false
     */
    virtual bool Open();

    /**
     * @brief 关闭卷
     */
    virtual void Close();

    /**
     * @brief 异步读请求
     * @param context 读请求信息
     */
    virtual void AioRead(NebdClientAioContext* context);

    /**
     * @brief 异步写请求
     * @param context 写请求信息
     */
    virtual void AioWrite(NebdClientAioContext* context);

    /**
     * @brief trim请求
     * @param context trim请求信息
     */
    virtual void Trim(NebdClientAioContext* context);

    /**
     * @brief flush请求
     * @param context flush请求信息
     */
    virtual void Flush(NebdClientAioContext* context);

    /**
     * @brief 获取文件大小
     * @return 获取成功返回文件大小（正值）
     *         获取失败返回错误码（负值）
     */
    virtual int64_t GetImageSize();

 private:
    // nebd返回的文件描述符
    int fd_;

    // 卷名
    std::string imageName_;
};
using ImagePtr = std::shared_ptr<ImageInstance>;

}  // namespace nbd
}  // namespace curve

#endif  // SRC_TOOLS_NBD_IMAGEINSTANCE_H_
