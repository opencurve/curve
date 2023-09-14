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

#ifndef NEBD_SRC_PART1_LIBNEBD_FILE_H_
#define NEBD_SRC_PART1_LIBNEBD_FILE_H_

#include "nebd/src/part1/libnebd.h"

/**
 * @brief initializes nebd and only executes the initialization logic on the first call
 * @param none
 * @return returns 0 for success, -1 for failure
 */
int Init4Nebd(const char* confpath);
/**
 * @brief uninitialize nebd
 * @param none
 * @return returns 0 for success, -1 for failure
 */
void Uninit4Nebd();
/**
 * @brief open file
 * @param filename: File name
 * @return successfully returned the file fd, but failed with an error code
 */
int Open4Nebd(const char* filename, const NebdOpenFlags* flags);
/**
 * @brief close file
 * @param fd: fd of the file
 * @return success returns 0, failure returns error code
 */
int Close4Nebd(int fd);
/**
 * @brief resize file
 * @param fd: fd of the file
 *         size: adjusted file size
 * @return success returns 0, failure returns error code
 */
int Extend4Nebd(int fd, int64_t newsize);
/**
 * @brief Get file size
 * @param fd: fd of the file
 * @return successfully returned the file size, but failed with an error code
 */
int64_t GetFileSize4Nebd(int fd);

int64_t GetBlockSize4Nebd(int fd);

/**
 * @brief discard file, asynchronous function
 * @param fd: fd of the file
 *         context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int Discard4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 * @brief Read file, asynchronous function
 * @param fd: fd of the file
 *         context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int AioRead4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 * @brief write file, asynchronous function
 * @param fd: fd of the file
 *         context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int AioWrite4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 * @brief flush file, asynchronous function
 * @param fd: fd of the file
 *         context: The context of an asynchronous request, including the information required for the request and the callback
 * @return success returns 0, failure returns error code
 */
int Flush4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 * @brief Get info of the file
 * @param fd: fd of the file
 * @return successfully returned the file object size, but failed with an error code
 */
int64_t GetInfo4Nebd(int fd);
/**
 * @brief refresh cache, wait for all asynchronous requests to return
 * @param fd: fd of the file
 * @return success returns 0, failure returns error code
 */
int InvalidCache4Nebd(int fd);

#endif  // NEBD_SRC_PART1_LIBNEBD_FILE_H_
