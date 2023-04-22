/*
 * @Author: hzwuhongsong hzwuhongsong@corp.netease.com
 * @Date: 2023-04-11 14:57:19
 * @LastEditors: hzwuhongsong hzwuhongsong@corp.netease.com
 * @LastEditTime: 2023-04-11 14:57:19
 * @FilePath: /curve/curvefs/src/client/cache/diskcache/disk_cache_base.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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

#ifndef CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_BASE_H_
#define CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_BASE_H_

#include <glog/logging.h>

#include <string>
#include <list>
#include <set>
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
                      const std::string cacheDir);
    /**
     * @brief Create Read/Write Cache Dir.
    */
    virtual int CreateIoDir(bool writreDir);
    virtual bool IsFileExist(const std::string file);
    /**
     * @brief Get Read/Write Cache full Dir(include CacheDir_).
    */
    virtual std::string GetCacheIoFullDir();

    virtual int LoadAllCacheFile(std::set<std::string> *cachedObj);

 private:
    std::string cacheIoDir_;
    std::string cacheDir_;

    // file system operation encapsulation
    std::shared_ptr<PosixWrapper> posixWrapper_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_BASE_H_
