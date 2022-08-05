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
 * Project: curve
 * Created Date: 20221103
 * Author: xuchaojie
 */

#ifndef SRC_CHUNKSERVER_SPDK_HOOK_H_
#define SRC_CHUNKSERVER_SPDK_HOOK_H_

#include <spdk/log.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <pfs_api.h>
#include <pfs_trace_func.h>
#include <pfsd.h>
#include <pfsd_sdk.h>
#include <pfsd_sdk_log.h>
#include <pfs_spdk_api.h>

void glog_pfs_func(int level, const char *file, const char *func, int line,
    const char *fmt, va_list ap);

void glog_spdk_func(int level, const char *a_file, const int a_line,
    const char *func, const char *fmt, va_list ap);

ssize_t glog_dpdk_log_func(void *cookie, const char *buf, size_t size);

void* dpdk_mem_allocate(size_t align, size_t sz);

void dpdk_mem_free(void* p);

#endif  // SRC_CHUNKSERVER_SPDK_HOOK_H_
