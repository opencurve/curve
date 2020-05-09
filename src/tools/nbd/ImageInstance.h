/**
 * Project: curve
 * Date: Thu Apr 23 09:50:18 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
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
