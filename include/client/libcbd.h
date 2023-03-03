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
 * Project: Curve
 *
 * History:
 *          2018/10/10  Wenyu Zhou   Initial version
 */

#ifndef INCLUDE_CLIENT_LIBCBD_H_
#define INCLUDE_CLIENT_LIBCBD_H_

#include "libcurve_define.h"  // NOLINT

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>

// #define CBD_BACKEND_FAKE

#ifndef CBD_BACKEND_FAKE
#define CBD_BACKEND_LIBCURVE
#else
#define CBD_BACKEND_EXT4
#endif

#define CBD_MAX_FILE_PATH_LEN 1024
#define CBD_MAX_BUF_LEN       1024 * 1024 * 32

typedef int CurveFd;

typedef struct CurveOptions {
    bool inited;
    char *conf;
#ifdef CBD_BACKEND_EXT4
    char *datahome;
#endif
} CurveOptions;

int cbd_ext4_init(const CurveOptions *options);
int cbd_ext4_fini(void);
int cbd_ext4_open(const char *filename);
int cbd_ext4_close(int fd);
int cbd_ext4_pread(int fd, void *buf, off_t offset, size_t length);
int cbd_ext4_pwrite(int fd, const void *buf, off_t offset, size_t length);
int cbd_ext4_pdiscard(int fd, off_t offset, size_t length);
int cbd_ext4_aio_pread(int fd, CurveAioContext *context);
int cbd_ext4_aio_pwrite(int fd, CurveAioContext *context);
int cbd_ext4_aio_pdiscard(int fd, CurveAioContext *context);
int cbd_ext4_sync(int fd);
int64_t cbd_ext4_filesize(const char *filename);
int cbd_ext4_increase_epoch(const char *filename);

int cbd_libcurve_init(const CurveOptions *options);
int cbd_libcurve_fini(void);
int cbd_libcurve_open(const char *filename);
int cbd_libcurve_close(int fd);
int cbd_libcurve_pread(int fd, void *buf, off_t offset, size_t length);
int cbd_libcurve_pwrite(int fd, const void *buf, off_t offset, size_t length);
int cbd_libcurve_pdiscard(int fd, off_t offset, size_t length);
int cbd_libcurve_aio_pread(int fd, CurveAioContext *context);
int cbd_libcurve_aio_pwrite(int fd, CurveAioContext *context);
int cbd_libcurve_aio_pdiscard(int fd, CurveAioContext *context);
int cbd_libcurve_sync(int fd);
int64_t cbd_libcurve_filesize(const char *filename);
int cbd_libcurve_resize(const char *filename, int64_t size);
int cbd_libcurve_increase_epoch(const char *filename);

#ifndef CBD_BACKEND_FAKE
#define cbd_lib_init           cbd_libcurve_init
#define cbd_lib_fini           cbd_libcurve_fini
#define cbd_lib_open           cbd_libcurve_open
#define cbd_lib_close          cbd_libcurve_close
#define cbd_lib_pread          cbd_libcurve_pread
#define cbd_lib_pwrite         cbd_libcurve_pwrite
#define cbd_lib_pdiscard       cbd_libcurve_pdiscard
#define cbd_lib_aio_pread      cbd_libcurve_aio_pread
#define cbd_lib_aio_pwrite     cbd_libcurve_aio_pwrite
#define cbd_lib_aio_pdiscard   cbd_libcurve_aio_pdiscard
#define cbd_lib_sync           cbd_libcurve_sync
#define cbd_lib_filesize       cbd_libcurve_filesize
#define cbd_lib_resize         cbd_libcurve_resize
#define cbd_lib_increase_epoch cbd_libcurve_increase_epoch
#else
#define cbd_lib_init           cbd_ext4_init
#define cbd_lib_fini           cbd_ext4_fini
#define cbd_lib_open           cbd_ext4_open
#define cbd_lib_close          cbd_ext4_close
#define cbd_lib_pread          cbd_ext4_pread
#define cbd_lib_pwrite         cbd_ext4_pwrite
#define cbd_lib_pdiscard       cbd_ext4_pdiscard
#define cbd_lib_aio_pread      cbd_ext4_aio_pread
#define cbd_lib_aio_pwrite     cbd_ext4_aio_pwrite
#define cbd_lib_aio_pdiscard   cbd_ext4_aio_pdiscard
#define cbd_lib_sync           cbd_ext4_sync
#define cbd_lib_filesize       cbd_ext4_filesize
#define cbd_lib_increase_epoch cbd_ext4_increase_epoch
#endif

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // INCLUDE_CLIENT_LIBCBD_H_
