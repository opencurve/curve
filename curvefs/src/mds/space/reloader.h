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
 * Date: Tuesday Mar 01 12:40:35 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_RELOADER_H_
#define CURVEFS_SRC_MDS_SPACE_RELOADER_H_

#include <atomic>
#include <memory>

#include "curvefs/src/mds/space/manager.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace mds {

class FsInfo;

namespace space {

class Reloader {
 public:
    Reloader(SpaceManager* manager, int concurrency);

    void Add(const FsInfo& fsInfo);

    SpaceErrCode Wait();

 private:
    void ReloadOne(const FsInfo& fsInfo);

 private:
    SpaceManager* manager_;
    std::unique_ptr<curve::common::TaskThreadPool<>> taskPool_;
    std::atomic<SpaceErrCode> error_;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_RELOADER_H_
