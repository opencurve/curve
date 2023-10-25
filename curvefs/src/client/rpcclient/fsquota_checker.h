/*
 *  Copyright (c) 2023 NetEase Inc.
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

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_FSQUOTA_CHECKER_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_FSQUOTA_CHECKER_H_

#include <atomic>

namespace curvefs {
namespace client {

class FsQuotaChecker {
 public:
    static FsQuotaChecker& GetInstance() {
        static FsQuotaChecker instance_;
        return instance_;
    }

    void Init();

    bool QuotaBytesCheck(uint64_t incBytes);

    void UpdateQuotaCache(uint64_t capacity, uint64_t usedBytes);

 private:
    std::atomic<uint64_t> fsCapacityCache_;
    std::atomic<uint64_t> fsUsedBytesCache_;
};

}  // namespace client

}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_FSQUOTA_CHECKER_H_
