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

const char* kCurveClientConfigPath = "/etc/curve/client.conf";

bool NebdImageInstance::Open() {
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

void NebdImageInstance::Close() {
    nebd_lib_close(fd_);
    fd_ = -1;
}

bool NebdImageInstance::AioRead(AioRequestContext* context) {
    nebd_lib_aio_pread(fd_, &(context->nebdAioCtx));

    return true;
}

bool NebdImageInstance::AioWrite(AioRequestContext* context) {
    nebd_lib_aio_pwrite(fd_, &(context->nebdAioCtx));

    return true;
}

bool NebdImageInstance::Trim(AioRequestContext* context) {
    nebd_lib_discard(fd_, &(context->nebdAioCtx));

    return true;
}

bool NebdImageInstance::Flush(AioRequestContext* context) {
    nebd_lib_flush(fd_, &(context->nebdAioCtx));

    return true;
}

int64_t NebdImageInstance::GetImageSize() {
    return nebd_lib_filesize(fd_);
}

NebdImageInstance::~NebdImageInstance() {
    if (fd_ != -1) {
        Close();
    }
}

bool CurveImageInstance::Open() {
    int ret = 0;
    ret = client_.Init(kCurveClientConfigPath);

    if (ret != 0) {
        LOG(ERROR) << "Init CurveClient failed, ret = " << ret;
        return false;
    }

    ret = client_.Open(imageName_, nullptr);
    if (ret != 0) {
        LOG(ERROR) << "Open failed, ret = " << ret;
        return false;
    }

    fd_ = ret;
    return true;
}

void CurveImageInstance::Close() {
    client_.Close(fd_);
}

bool CurveImageInstance::AioRead(AioRequestContext* context) {
    return LIBCURVE_ERROR::OK == client_.AioRead(fd_, &(context->curveAioCtx));
}

bool CurveImageInstance::AioWrite(AioRequestContext* context) {
    return LIBCURVE_ERROR::OK == client_.AioWrite(fd_, &(context->curveAioCtx));
}

bool CurveImageInstance::Flush(AioRequestContext* context) {
    context->curveAioCtx.ret = 0;
    context->curveAioCtx.cb(&(context->curveAioCtx));

    return true;
}

bool CurveImageInstance::Trim(AioRequestContext* context) {
    context->curveAioCtx.ret = 0;
    context->curveAioCtx.cb(&(context->curveAioCtx));

    return true;
}

int64_t CurveImageInstance::GetImageSize() {
    return client_.StatFile(imageName_);
}

CurveImageInstance::~CurveImageInstance() {
    if (fd_ != -1) {
        Close();
        fd_ = -1;
    }

    client_.UnInit();
}

}  // namespace nbd
}  // namespace curve
