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
 * Created Date: 2021-02-03
 * Author: qinyi
 */

#ifndef SRC_CHUNKSERVER_WATCHDOG_DOG_KEEPER_H_
#define SRC_CHUNKSERVER_WATCHDOG_DOG_KEEPER_H_

#include <string.h>

#include <iostream>
#include <memory>
#include <thread>

#include "src/chunkserver/watchdog/common.h"
#include "src/chunkserver/watchdog/dog.h"

namespace curve {
namespace chunkserver {
class DogKeeper {
 public:
    DogKeeper(std::shared_ptr<Dog> dog, const WatchConf conf)
        : dog_(dog), stallSec_(0), conf_(conf), destroy_(false) {}

    ~DogKeeper() {
        SetDestroy();
        if (thread_.joinable())
            thread_.join();
    }

    int Init();

    /* start dog and keeper thread */
    int Run();

    bool IsDestroy() const;

    void SetDestroy();

    /*
     *  user may update config anytime:
     *  curl -L chunkServerIp:port/flags/killChunkserverOnErr?setvalue=true
     */
    void UpdateConfig();

    /*
     * feed dog every second,
     * if a dog have not eaten food over limited times, set sick
     */
    void FeedDog();
    void CheckDog();
    bool IsDogDead() const;
    void ReviveDog();

 public:
    WatchConf conf_;

 protected:
    std::shared_ptr<Dog> dog_;
    uint32_t stallSec_;
    std::thread thread_;
    volatile bool destroy_;
};  // class DogKeeper

}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_WATCHDOG_DOG_KEEPER_H_
