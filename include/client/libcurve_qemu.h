/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:07:05 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_INTERFACE_H
#define CURVE_LIBCURVE_INTERFACE_H

#include <unistd.h>
#include <stdint.h>
#include <vector>
#include <map>

#include "src/client/libcurve_define.h"

#ifdef __cplusplus
extern "C" {
#endif

LIBCURVE_ERROR Init(const char* path);
int Open(const char* filename, size_t size, bool create);

// 同步读写
LIBCURVE_ERROR Read(int fd, char* buf, off_t offset, size_t length);
LIBCURVE_ERROR Write(int fd, const char* buf, off_t offset, size_t length);

// 异步读写
LIBCURVE_ERROR AioRead(int fd, CurveAioContext* aioctx);
LIBCURVE_ERROR AioWrite(int fd, CurveAioContext* aioctx);

// 获取文件的基本信息
LIBCURVE_ERROR StatFs(int fd, FileStatInfo* finfo);
void Close(int fd);

void UnInit();

#ifdef __cplusplus
}
#endif

#endif  // !CURVE_LIBCURVE_INTERFACE_H
