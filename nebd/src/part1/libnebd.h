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

//The maximum length of the file path, in bytes
#define NEBD_MAX_FILE_PATH_LEN   1024

//Types of nebd asynchronous requests
typedef enum LIBAIO_OP {
    LIBAIO_OP_READ,
    LIBAIO_OP_WRITE,
    LIBAIO_OP_DISCARD,
    LIBAIO_OP_FLUSH,
} LIBAIO_OP;

typedef struct NebdOpenFlags {
    int exclusive;
} NebdOpenFlags;

void nebd_lib_init_open_flags(NebdOpenFlags* flags);

struct NebdClientAioContext;

//The type of nebd callback function
typedef void (*LibAioCallBack)(struct NebdClientAioContext* context);

struct NebdClientAioContext {
    off_t offset;             //Requested offset
    size_t length;            //Requested length
    int ret;                  //Record the return value returned asynchronously
    LIBAIO_OP op;             //The type of asynchronous request, as defined in the definition
    LibAioCallBack cb;        //Callback function for asynchronous requests
    void* buf;                //Buf requested
    unsigned int retryCount;  //Record the number of retries for asynchronous requests
};

// int nebd_lib_fini(void);
/**
 * @brief initializes nebd and only executes the initialization logic on the first call
 * @param none
 * @return returns 0 for success, -1 for failure
 */
int nebd_lib_init(void);

int nebd_lib_init_with_conf(const char* confPath);

/**
 * @brief uninitialize nebd
 * @param none
 * @return returns 0 for success, -1 for failure
 */
int nebd_lib_uninit(void);

/**
 * @brief open file
 * @param filename: File name
 * @return successfully returned the file fd, but failed with an error code
 */
int nebd_lib_open(const char* filename);
int nebd_lib_open_with_flags(const char* filename,
                             const NebdOpenFlags* openflags);

/**
 * @brief close file
 * @param fd: fd of the file
 * @return success returns 0, failure returns error code
 */
int nebd_lib_close(int fd);

/**
 * @brief Synchronize file reading
 * @param fd: fd of the file
 *          buf: Store and read data buf
 *          offset: The position read offset
 *          length: The length read
 * @return success returns 0, failure returns error code
 */
int nebd_lib_pread(int fd, void* buf, off_t offset, size_t length);

/**
 * @brief Synchronize file writing
 * @param fd: fd of the file
 *          buf: Store and read data buf
 *          offset: The position read offset
 *          length: The length read
 * @return success returns 0, failure returns error code
 */
int nebd_lib_pwrite(int fd, const void* buf, off_t offset, size_t length);

/**
 * @brief discard file, asynchronous function
 * @param fd: fd of the file
 *          context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int nebd_lib_discard(int fd, struct NebdClientAioContext* context);

/**
 * @brief Read file, asynchronous function
 * @param fd: fd of the file
 *          context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int nebd_lib_aio_pread(int fd, struct NebdClientAioContext* context);

/**
 * @brief write file, asynchronous function
 * @param fd: fd of the file
 *          context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int nebd_lib_aio_pwrite(int fd, struct NebdClientAioContext* context);

/**
 * @brief sync file
 * @param fd: fd of the file
 * @return success returns 0, failure returns error code
 */
int nebd_lib_sync(int fd);

/**
 * @brief Get file size
 * @param fd: fd of the file
 * @return successfully returned the file size, but failed with an error code
 */
int64_t nebd_lib_filesize(int fd);

int64_t nebd_lib_blocksize(int fd);

/**
 * @brief resize file
 * @param fd: fd of the file
 *          size: adjusted file size
 * @return success returns 0, failure returns error code
 */
int nebd_lib_resize(int fd, int64_t size);

/**
 * @brief flush file, asynchronous function
 * @param fd: fd of the file
 *          context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int nebd_lib_flush(int fd, struct NebdClientAioContext* context);

/**
 * @brief Get file information
 * @param fd: fd of the file
 * @return successfully returned the file object size, but failed with an error code
 */
int64_t nebd_lib_getinfo(int fd);

/**
 * @brief refresh cache, wait for all asynchronous requests to return
 * @param fd: fd of the file
 * @return success returns 0, failure returns error code
 */
int nebd_lib_invalidcache(int fd);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // NEBD_SRC_PART1_LIBNEBD_H_
