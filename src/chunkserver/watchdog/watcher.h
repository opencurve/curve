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
 * Created Date: 2020-12-11
 * Author: qinyi
 */

#ifndef SRC_CHUNKSERVER_WATCHDOG_WATCHER_H_
#define SRC_CHUNKSERVER_WATCHDOG_WATCHER_H_

#include <memory>

#include "src/chunkserver/watchdog/dog.h"
#include "src/chunkserver/watchdog/common.h"
#include "src/chunkserver/watchdog/helper.h"

namespace curve {
namespace chunkserver {

class Watcher {
 public:
    Watcher(Dog* dog, WatcherName watcherName, const WatchConf* config,
            std::shared_ptr<WatchdogHelper> helper)
        : dog_(dog),
          watcherName_(watcherName),
          config_(config),
          helper_(helper) {}
    virtual int Run() = 0;

 public:
    const Dog* dog_;

 protected:
    const WatcherName watcherName_;
    const WatchConf* config_;
    std::shared_ptr<WatchdogHelper> helper_;
};

class DiskSmartWatcher : public Watcher {
 public:
    DiskSmartWatcher(Dog* dog, WatcherName watcherName,
                     const WatchConf* config,
                     std::shared_ptr<WatchdogHelper> helper)
        : Watcher(dog, watcherName, config, helper) {}
    int Run();
};

class FileOpWatcher : public Watcher {
 public:
    FileOpWatcher(Dog* dog, WatcherName watcherName,
                  const WatchConf* config,
                  std::shared_ptr<WatchdogHelper> helper)
        : Watcher(dog, watcherName, config, helper) {}
    int Run();

 private:
    int WriteData(int fd, const char* buf, uint64_t offset, int length);
    int ReadData(int fd, char* buf, uint64_t offset, int length);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_WATCHDOG_WATCHER_H_
