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

#ifndef SRC_CHUNKSERVER_WATCHDOG_DOG_H_
#define SRC_CHUNKSERVER_WATCHDOG_DOG_H_

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>

#include "src/chunkserver/watchdog/common.h"
#include "src/chunkserver/watchdog/helper.h"


namespace curve {
namespace chunkserver {

class Watcher;

class Dog {
 public:
    /* create dog thread for server */
    explicit Dog(std::shared_ptr<WatchdogHelper> helper)
        : helper_(helper),
          config_(nullptr),
          health_(DogState::DOG_DEAD),
          food_(0),
          destroy_(false) {}

    ~Dog() {
        SetDestroy();
        if (thread_.joinable())
            thread_.join();
    }

    int Init(const WatchConf* config);
    int Run();

    /* eat food after each operation, to prove it's healthy */
    void Eat();

    /*
     * refresh food stock,
     * return 0 if food is eaten, else return 1
     */
    int Feed();

    /*
     * sick means some error occurred for dog, but not handled
     * dead means some error occurred for dog, and already handled
    */
    bool IsHealthy() const;
    bool IsSick() const;
    bool IsDead() const;

    void Revive();
    void SetSick();
    void SetDead();
    bool IsDestroy() const;
    void SetDestroy();

    void OnError();

 private:
    string PathToDeviceName(const string* path);

 public:
    const WatchConf* config_;
    std::vector<std::shared_ptr<Watcher>> watchers_;

 private:
    std::shared_ptr<WatchdogHelper> helper_;

    /*
     * if error detected, dog will be set health to DOG_SICK
     */
    volatile DogState health_;

    /*
     * dog must eat food in cycles, to prove it is fine;
     * otherwise, if it is blocked in any test operation,
     * it will be marked sick.
     */
    volatile int food_;

    std::thread thread_;
    volatile bool destroy_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_WATCHDOG_DOG_H_
