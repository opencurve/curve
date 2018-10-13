/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/10/10  Wenyu Zhou   Initial version
 */

#ifndef __LIBCBD_H__
#define __LIBCBD_H__

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

#define CBD_MAX_FILE_PATH_LEN   1024
#define CBD_MAX_BUF_LEN         1024 * 1024 * 32

typedef int CurveFd;

struct CurveAioContext;
typedef struct CurveOptions {
    bool    inited;
    char*   conf;
#ifdef CBD_BACKEND_EXT4
    char*   datahome;
#endif
} CurveOptions;

int cbd_ext4_init(const CurveOptions* options);
int cbd_ext4_fini(void);
int cbd_ext4_open(const char* filename);
int cbd_ext4_close(int fd);
int cbd_ext4_pread(int fd, void* buf, off_t offset, size_t length);
int cbd_ext4_pwrite(int fd, const void* buf, off_t offset, size_t length);
int cbd_ext4_aio_pread(int fd, CurveAioContext* context);
int cbd_ext4_aio_pwrite(int fd, CurveAioContext* context);
int cbd_ext4_sync(int fd);
int64_t cbd_ext4_filesize(const char* filename);

int cbd_libcurve_init(const CurveOptions* options);
int cbd_libcurve_fini(void);
int cbd_libcurve_open(const char* filename);
int cbd_libcurve_close(int fd);
int cbd_libcurve_pread(int fd, void* buf, off_t offset, size_t length);
int cbd_libcurve_pwrite(int fd, const void* buf, off_t offset, size_t length);
int cbd_libcurve_aio_pread(int fd, CurveAioContext* context);
int cbd_libcurve_aio_pwrite(int fd, CurveAioContext* context);
int cbd_libcurve_sync(int fd);
int64_t cbd_libcurve_filesize(const char* filename);

#ifndef CBD_BACKEND_FAKE
#define cbd_lib_init        cbd_libcurve_init
#define cbd_lib_fini        cbd_libcurve_fini
#define cbd_lib_open        cbd_libcurve_open
#define cbd_lib_close       cbd_libcurve_close
#define cbd_lib_pread       cbd_libcurve_pread
#define cbd_lib_pwrite      cbd_libcurve_pwrite
#define cbd_lib_aio_pread   cbd_libcurve_aio_pread
#define cbd_lib_aio_pwrite  cbd_libcurve_aio_pwrite
#define cbd_lib_sync        cbd_libcurve_sync
#define cbd_lib_filesize    cbd_libcurve_filesize
#else
#define cbd_lib_init        cbd_ext4_init
#define cbd_lib_fini        cbd_ext4_fini
#define cbd_lib_open        cbd_ext4_open
#define cbd_lib_close       cbd_ext4_close
#define cbd_lib_pread       cbd_ext4_pread
#define cbd_lib_pwrite      cbd_ext4_pwrite
#define cbd_lib_aio_pread   cbd_ext4_aio_pread
#define cbd_lib_aio_pwrite  cbd_ext4_aio_pwrite
#define cbd_lib_sync        cbd_ext4_sync
#define cbd_lib_filesize    cbd_ext4_filesize
#endif

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // __LIBCBD_H__
