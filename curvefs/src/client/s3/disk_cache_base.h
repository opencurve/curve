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
 * Created Date: 21-08-13
 * Author: wuhongsong
 */

#ifndef CURVEFS_SRC_CLIENT_S3_DISK_CACHE_BASE_H_
#define CURVEFS_SRC_CLIENT_S3_DISK_CACHE_BASE_H_

#include <glog/logging.h>

#include <string>
#include <list>
#include <set>
#include <vector>
#include <memory>

#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/client/metric/client_metric.h"

namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;
using curvefs::client::metric::DiskCacheMetric;
#define MODE 0644

class DiskCacheBase {
 public:
    DiskCacheBase() {}
    virtual ~DiskCacheBase() {}
    virtual void Init(std::shared_ptr<PosixWrapper> wrapper,
                      const std::string cacheDir, uint32_t objectPrefix);
    /**
     * @brief Create Read/Write Cache Dir.
    */
    virtual int CreateIoDir(bool writreDir);
    virtual bool IsFileExist(const std::string file);
    virtual int CreateDir(const std::string name);
    /**
     * @brief Get Read/Write Cache full Dir(include CacheDir_).
    */
    virtual std::string GetCacheIoFullDir();

    virtual int LoadAllCacheFile(std::set<std::string> *cachedObj);
    uint32_t objectPrefix_;

 private:
    std::string cacheIoDir_;
    std::string cacheDir_;
    // file system operation encapsulation
    std::shared_ptr<PosixWrapper> posixWrapper_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_BASE_H_
