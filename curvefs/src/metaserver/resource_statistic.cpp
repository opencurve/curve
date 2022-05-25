/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Saturday Jun 11 14:34:37 CST 2022
 * Author: wuhanqing
 */

// Copyright (c) 2015 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Author: Ge,Jun (gejun@baidu.com)
// Date: Thu Jul 30 17:44:54 CST 2015

#include "curvefs/src/metaserver/resource_statistic.h"

#include <butil/files/scoped_file.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <limits>
#include <mutex>
#include <utility>

namespace curvefs {
namespace metaserver {

namespace {

// Corresponding to fields in /proc/self/statm
// Provides information about memory usage, measured in pages
struct ProcMemory {
    int64_t size;      // total program size
    int64_t resident;  // resident set size
    int64_t share;     // shared pages
    int64_t trs;       // text (code)
    int64_t lrs;       // library
    int64_t drs;       // data/stack
    int64_t dt;        // dirty pages
};

const char* const kProcStatmPath = "/proc/self/statm";

bool ReadProcMemory(ProcMemory* mem) {
    butil::ScopedFILE fp(kProcStatmPath, "r");
    if (nullptr == fp) {
        LOG(WARNING) << "Failed to open " << kProcStatmPath;
        return false;
    }

    if (fscanf(fp, "%ld %ld %ld %ld %ld %ld %ld", &mem->size, &mem->resident,
               &mem->share, &mem->trs, &mem->lrs, &mem->drs, &mem->dt) != 7) {
        LOG(WARNING) << "Failed to fscanf " << kProcStatmPath;
        return false;
    }

    return true;
}

bool GetProcRssMemory(uint64_t* bytes) {
    ProcMemory mem{};
    if (!ReadProcMemory(&mem)) {
        return false;
    }

    static const int kPageSize = getpagesize();
    *bytes = static_cast<uint64_t>(mem.resident) * kPageSize;
    return true;
}

bool GetFileSystemSpaces(const std::string& path,
                         uint64_t* total,
                         uint64_t* available) {
    struct statfs st;
    if (statfs(path.c_str(), &st) != 0) {
        LOG(WARNING) << "Failed to statfs, error: " << errno;
        return false;
    }

    *total = st.f_blocks * st.f_bsize;
    *available = st.f_bavail * st.f_bsize;
    return true;
}

}  // namespace

ResourceCollector::ResourceCollector(uint64_t diskQuota,
                                     uint64_t memQuota,
                                     std::string dataRoot)
    : diskQuotaBytes_(diskQuota),
      memQuotaBytes_(memQuota),
      dataRoot_(std::move(dataRoot)) {}

void ResourceCollector::SetDataRoot(const std::string& dataRoot) {
    dataRoot_ = dataRoot;
}

void ResourceCollector::SetDiskQuota(uint64_t diskQuotaBytes) {
    diskQuotaBytes_ = diskQuotaBytes;
}

void ResourceCollector::SetMemoryQuota(uint64_t memQuotaBytes) {
    memQuotaBytes_ = memQuotaBytes;
}

bool ResourceCollector::GetResourceStatistic(StorageStatistics* statistics) {
    statistics->maxDiskQuotaBytes = diskQuotaBytes_;
    statistics->maxMemoryQuotaBytes = memQuotaBytes_;

    uint64_t totalDiskBytes = 0;
    uint64_t availDiskBytes = 0;
    if (!GetFileSystemSpaces(dataRoot_, &totalDiskBytes, &availDiskBytes)) {
        return false;
    }

    statistics->diskUsageBytes = totalDiskBytes - availDiskBytes;
    return GetProcRssMemory(&statistics->memoryUsageBytes);
}

}  // namespace metaserver
}  // namespace curvefs
