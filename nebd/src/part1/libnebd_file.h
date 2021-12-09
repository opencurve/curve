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
 *  @brief 初始化nebd，仅在第一次调用的时候真正执行初始化逻辑
 *  @param none
 *  @return 成功返回0，失败返回-1
 */
int Init4Nebd(const char* confpath);
/**
 *  @brief 反初始化nebd
 *  @param none
 *  @return 成功返回0，失败返回-1
 */
void Uninit4Nebd();
/**
 *  @brief open文件
 *  @param filename：文件名
 *  @return 成功返回文件fd，失败返回错误码
 */
int Open4Nebd(const char* filename, const NebdOpenFlags* flags);
/**
 *  @brief close文件
 *  @param fd：文件的fd
 *  @return 成功返回0，失败返回错误码
 */
int Close4Nebd(int fd);
/**
 *  @brief resize文件
 *  @param fd：文件的fd
 *         size：调整后的文件size
 *  @return 成功返回0，失败返回错误码
 */
int Extend4Nebd(int fd, int64_t newsize);
/**
 *  @brief 获取文件size
 *  @param fd：文件的fd
 *  @return 成功返回文件size，失败返回错误码
 */
int64_t GetFileSize4Nebd(int fd);
/**
 *  @brief discard文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int Discard4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 *  @brief 读文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int AioRead4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 *  @brief 写文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int AioWrite4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 *  @brief flush文件，异步函数
 *  @param fd：文件的fd
 *         context：异步请求的上下文，包含请求所需的信息以及回调
 *  @return 成功返回0，失败返回错误码
 */
int Flush4Nebd(int fd, NebdClientAioContext* aioctx);
/**
 *  @brief 获取文件info
 *  @param fd：文件的fd
 *  @return 成功返回文件对象size，失败返回错误码
 */
int64_t GetInfo4Nebd(int fd);
/**
 *  @brief 刷新cache，等所有异步请求返回
 *  @param fd：文件的fd
 *  @return 成功返回0，失败返回错误码
 */
int InvalidCache4Nebd(int fd);

#endif  // NEBD_SRC_PART1_LIBNEBD_FILE_H_
