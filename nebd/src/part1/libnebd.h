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
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 */

#ifndef NEBD_SRC_PART1_LIBNEBD_H_
#define NEBD_SRC_PART1_LIBNEBD_H_

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

// 文件路径最大的长度，单位字节
#define NEBD_MAX_FILE_PATH_LEN   1024

// nebd异步请求的类型
typedef enum LIBAIO_OP {
    LIBAIO_OP_READ,
    LIBAIO_OP_WRITE,
    LIBAIO_OP_DISCARD,
    LIBAIO_OP_FLUSH,
} LIBAIO_OP;

struct NebdClientAioContext;

// nebd回调函数的类型
typedef void (*LibAioCallBack)(struct NebdClientAioContext* context);

struct NebdClientAioContext {
    off_t offset;             // 请求的offset
    size_t length;            // 请求的length
    int ret;                  // 记录异步返回的返回值
    LIBAIO_OP op;             // 异步请求的类型，详见定义
    LibAioCallBack cb;        // 异步请求的回调函数
    void* buf;                // 请求的buf
    unsigned int retryCount;  // 记录异步请求的重试次数
};

// int nebd_lib_fini(void);
// for ceph & curve
/**
 *  @brief 初始化nebd，仅在第一次调用的时候真正执行初始化逻辑
 *  @param none
 *  @return 成功返回0，失败返回-1
 */
int nebd_lib_init(void);

/**
 *  @brief 反初始化nebd
 *  @param none
 *  @return 成功返回0，失败返回-1
 */
int nebd_lib_uninit(void);

/**
 *  @brief open文件
 *  @param filename：文件名
 *  @return 成功返回文件fd，失败返回错误码
 */
int nebd_lib_open(const char* filename);

/**
 *  @brief close文件
 *  @param fd：文件的fd
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_close(int fd);

/**
 *  @brief 同步读文件
 *  @param fd：文件的fd
 *         buf：存放读取data的buf
 *         offset：读取的位置offset
 *         length：读取的长度
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_pread(int fd, void* buf, off_t offset, size_t length);

/**
 *  @brief 同步写文件
 *  @param fd：文件的fd
 *         buf：存放写入data的buf
 *         offset：写入的位置offset
 *         length：写入的长度
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_pwrite(int fd, const void* buf, off_t offset, size_t length);

/**
 *  @brief discard文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_discard(int fd, NebdClientAioContext* context);

/**
 *  @brief 读文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_aio_pread(int fd, NebdClientAioContext* context);

/**
 *  @brief 写文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_aio_pwrite(int fd, NebdClientAioContext* context);

/**
 *  @brief sync文件
 *  @param fd：文件的fd
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_sync(int fd);

/**
 *  @brief 获取文件size
 *  @param fd：文件的fd
 *  @return 成功返回文件size，失败返回错误码
 */
int64_t nebd_lib_filesize(int fd);

/**
 *  @brief resize文件
 *  @param fd：文件的fd
 *         size：调整后的文件size
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_resize(int fd, int64_t size);

// for ceph only
/**
 *  @brief flush文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_flush(int fd, NebdClientAioContext* context);

/**
 *  @brief 获取文件info
 *  @param fd：文件的fd
 *  @return 成功返回文件对象size，失败返回错误码
 */
int64_t nebd_lib_getinfo(int fd);

/**
 *  @brief 刷新cache，等所有异步请求返回
 *  @param fd：文件的fd
 *  @return 成功返回0，失败返回错误码
 */
int nebd_lib_invalidcache(int fd);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // NEBD_SRC_PART1_LIBNEBD_H_
