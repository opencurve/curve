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

#include "nbd/src/ImageInstance.h"
#include <glog/logging.h>

namespace curve {
namespace nbd {

bool ImageInstance::Open() {
    int ret = 0;

    if (config_ && !config_->nebd_conf.empty()) {
        ret = nebd_lib_init_with_conf(config_->nebd_conf.c_str());
    } else {
        ret = nebd_lib_init();
    }

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
