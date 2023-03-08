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

#include <utility>

namespace curve {
namespace nbd {

bool NebdImage::Open() {
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

    NebdOpenFlags flags;
    nebd_lib_init_open_flags(&flags);
    flags.exclusive = config_->exclusive ? 1 : 0;
    ret = nebd_lib_open_with_flags(imageName_.c_str(), &flags);
    if (ret < 0) {
        LOG(ERROR) << "open image failed, ret = " << ret;
        return false;
    }

    fd_ = ret;
    return true;
}

void NebdImage::Close() {
    if (fd_ != -1) {
        nebd_lib_close(fd_);
        fd_ = -1;
    }
}

bool NebdImage::AioRead(AioContext* context) {
    nebd_lib_aio_pread(fd_, &(context->nebd));
    return true;
}

bool NebdImage::AioWrite(AioContext* context) {
    nebd_lib_aio_pwrite(fd_, &(context->nebd));
    return true;
}

bool NebdImage::Trim(AioContext* context) {
    nebd_lib_discard(fd_, &(context->nebd));
    return true;
}

bool NebdImage::Flush(AioContext* context) {
    nebd_lib_flush(fd_, &(context->nebd));
    return true;
}

int64_t NebdImage::GetImageSize() {
    return nebd_lib_filesize(fd_);
}

NebdImage::~NebdImage() {
    Close();
    nebd_lib_uninit();
}

std::string CurveImage::TransformImageName(const std::string& name) {
    if (name.compare(0, 4, "cbd:") != 0) {
        return {};
    }

    auto pos = name.find_first_of('/');
    if (pos == std::string::npos) {
        return {};
    }

    return name.substr(pos + 1);
}

bool CurveImage::Open() {
    int ret = 0;
    ret = client_.Init(config_->curve_conf);

    if (ret != 0) {
        LOG(ERROR) << "Init CurveClient failed, ret = " << ret;
        return false;
    }

    curve::client::OpenFlags flags;
    flags.exclusive = config_->exclusive;

    std::string name = TransformImageName(imageName_);
    if (name.empty()) {
        LOG(ERROR) << "Open failed, filename `" << imageName_ << "` is invalid";
        return false;
    }

    ret = client_.Open(name, flags);
    if (ret != 0) {
        LOG(ERROR) << "Open failed, ret = " << ret;
        return false;
    }

    actualImageName_ = std::move(name);
    fd_ = ret;
    return true;
}

void CurveImage::Close() {
    if (fd_ != -1) {
        client_.Close(fd_);
        fd_ = -1;
    }
}

bool CurveImage::AioRead(AioContext* context) {
    return LIBCURVE_ERROR::OK ==
           client_.AioRead(fd_, &(context->curve),
                           curve::client::UserDataType::RawBuffer);
}

bool CurveImage::AioWrite(AioContext* context) {
    return LIBCURVE_ERROR::OK ==
           client_.AioWrite(fd_, &(context->curve),
                            curve::client::UserDataType::RawBuffer);
}

bool CurveImage::Flush(AioContext* context) {
    // curve-sdk doesn't have cache, so return directly
    context->curve.ret = 0;
    context->curve.cb(&(context->curve));

    return true;
}

bool CurveImage::Trim(AioContext* context) {
    // curve doesn't support trim yet
    context->curve.ret = 0;
    context->curve.cb(&(context->curve));

    return true;
}

int64_t CurveImage::GetImageSize() {
    return client_.StatFile(actualImageName_);
}

CurveImage::~CurveImage() {
    Close();
    client_.UnInit();
}

}  // namespace nbd
}  // namespace curve
