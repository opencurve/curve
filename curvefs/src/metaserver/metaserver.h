/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_METASERVER_H_
#define CURVEFS_SRC_METASERVER_METASERVER_H_

#include <memory>
#include <string>
#include "curvefs/src/metaserver/metaserver_service.h"
#include "src/common/configuration.h"

using ::curve::common::Configuration;

namespace curvefs {
namespace metaserver {
struct MetaserverOptions {
    std::string metaserverListenAddr;
};

class Metaserver {
 public:
    void InitOptions(std::shared_ptr<Configuration> conf);
    void Init();
    void Run();
    void Stop();

 private:
    // metaserver configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_;
    // running as the main MDS or not
    bool running_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<Trash> trash_;
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<DentryManager> dentryManager_;
    MetaserverOptions options_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVER_H_
