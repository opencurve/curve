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

#include <glog/logging.h>

#include "src/chunkserver/spdk_hook.h"

void glog_pfs_func(int level, const char *file, const char *func, int line,
    const char *fmt, va_list ap) {
    char buf[8192];

    int glevel = google::GLOG_INFO;
    vsnprintf(buf, sizeof(buf), fmt, ap);
    switch (level) {
    case PFS_TRACE_FATAL:
        glevel = google::GLOG_FATAL;
        google::LogMessage(file, line, glevel).stream() << buf;
        abort();
        break;
    case PFS_TRACE_ERROR:
        glevel = google::GLOG_ERROR;
        break;
    case PFS_TRACE_WARN:
        glevel = google::GLOG_WARNING;
        break;
    case PFS_TRACE_INFO:
    case PFS_TRACE_DBG:
    case PFS_TRACE_VERB:
    default:
        glevel = google::GLOG_INFO;
        break;
    }
    google::LogMessage(file, line, glevel).stream() << buf;
}

void glog_spdk_func(int level, const char *a_file, const int a_line,
    const char *func, const char *fmt, va_list ap) {
    char buf[8192];
    const char *file = a_file;
    int line = a_line;

    if (file == NULL) {
        file = "<spdk>";
        if (a_line <= 0)
            line = 1;
    }

    int glevel = google::GLOG_INFO;
    vsnprintf(buf, sizeof(buf), fmt, ap);
    switch (level) {
    case SPDK_LOG_ERROR:
        glevel = google::GLOG_ERROR;
        break;
    case SPDK_LOG_WARN:
        glevel = google::GLOG_WARNING;
        break;
    case SPDK_LOG_NOTICE:
    case SPDK_LOG_INFO:
        glevel = google::GLOG_INFO;
        break;
#ifndef NDEBUG
    case SPDK_LOG_DEBUG:
        glevel = google::GLOG_INFO;
        break;
#endif
    }
    google::LogMessage(file, line, glevel).stream() << buf;
}

ssize_t glog_dpdk_log_func(void *cookie, const char *buf, size_t size) {
    int level = rte_log_cur_msg_loglevel();
    int glevel = google::GLOG_INFO;

    switch (level) {
    case RTE_LOG_EMERG:
    case RTE_LOG_ALERT:
    case RTE_LOG_CRIT:
    case RTE_LOG_ERR:
        glevel = google::GLOG_ERROR;
        break;
    case RTE_LOG_WARNING:
        glevel = google::GLOG_WARNING;
        break;
    case RTE_LOG_NOTICE:
    case RTE_LOG_INFO:
    default:
        glevel = google::GLOG_INFO;
        break;
#ifndef NDEBUG
    case RTE_LOG_DEBUG:
        glevel = google::GLOG_INFO;
        return 0;
#endif
    }
    google::LogMessage("<dpdk>", 0, glevel).stream().write(buf, size);
    if (level == RTE_LOG_EMERG)
        abort();
    return size;
}

void* dpdk_mem_allocate(size_t align, size_t sz) {
    /* rte_malloc seems fast enough, otherwise we need to use mempool */
    return rte_malloc("iobuf", sz, align);
}

void dpdk_mem_free(void* p) {
    rte_free(p);
}
