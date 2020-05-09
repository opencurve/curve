/**
 * Project: curve
 * Date: Thu Apr 23 09:50:18 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#include "src/tools/nbd/ImageInstance.h"
#include <glog/logging.h>

namespace curve {
namespace nbd {

bool ImageInstance::Open() {
    int ret = 0;

    ret = nebd_lib_init();
    if (ret != 0) {
        LOG(ERROR) << "init nebd failed, ret = " << ret;
        return false;
    }

    ret = nebd_lib_open(imageName_.c_str());
    if (ret < 0) {
        LOG(ERROR) << "open image failed, ret = " << ret;
        return false;
    }

    fd_ = ret;
    return true;
}

void ImageInstance::Close() {
    nebd_lib_close(fd_);
    fd_ = -1;
}

void ImageInstance::AioRead(NebdClientAioContext* context) {
    nebd_lib_aio_pread(fd_, context);
}

void ImageInstance::AioWrite(NebdClientAioContext* context) {
    nebd_lib_aio_pwrite(fd_, context);
}

void ImageInstance::Trim(NebdClientAioContext* context) {
    nebd_lib_discard(fd_, context);
}

void ImageInstance::Flush(NebdClientAioContext* context) {
    nebd_lib_flush(fd_, context);
}

int64_t ImageInstance::GetImageSize() {
    return nebd_lib_filesize(fd_);
}

ImageInstance::~ImageInstance() {
    if (fd_ != -1) {
        Close();
    }
}

}  // namespace nbd
}  // namespace curve
