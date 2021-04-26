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

#include "src/chunkserver/watchdog/dog.h"

#include <mntent.h>
#include <signal.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "src/chunkserver/watchdog/watcher.h"

namespace curve {
namespace chunkserver {

void Watch(Dog* dog) {
    LOG(INFO) << "dog thread is running";

    while (true) {
        for (auto iter = dog->watchers_.begin(); iter != dog->watchers_.end();
             ++iter) {
            if (dog->IsDestroy()) {
                LOG(INFO) << "dog thread destroyed";
                return;
            }

            if (dog->IsSick() || dog->IsDead()) break;

            dog->Eat();
            if ((*iter)->Run() < 0) {
                dog->SetSick();
                break;
            }
        }

        for (int i = 0;
             i < dog->config_->watchdogCheckPeriodSec * kWatchdogSleepFreq;
             i++) {
            if (dog->IsDestroy()) {
                LOG(INFO) << "dog thread destroyed";
                return;
            }
            /* keep eating in sleep times */
            dog->Eat();
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kWatchdogSleepMs));
        }
    }

    LOG(ERROR) << "dog thread unexpected ending";

    return;
}

int Dog::Init(const WatchConf* config) {
    config_ = config;

    std::shared_ptr<DiskSmartWatcher> diskSmartWatcher =
        std::make_shared<DiskSmartWatcher>(
            this, WatcherName::DISK_SMART_WATCHER, config_, helper_);
    watchers_.push_back(diskSmartWatcher);

    std::shared_ptr<FileOpWatcher> fileOpWatcher =
        std::make_shared<FileOpWatcher>(this, WatcherName::FILE_OP_WATCHER,
                                        config_, helper_);
    watchers_.push_back(fileOpWatcher);

    health_ = DogState::DOG_HEALTHY;

    return 0;
}

int Dog::Run() {
    if (!config_ || IsDead()) {
        LOG(ERROR) << "unable to run watchdog, should init dog first";
        return -1;
    }

    thread_ = std::thread(Watch, this);
    return 0;
}

/* eat food after each operation, to prove it's healthy */
void Dog::Eat() {
    if (IsHealthy()) food_ = 0;
}

int Dog::Feed() {
    if (IsSick() || IsDead()) return 0;

    if (food_ == 0) {
        /* food is eaten up, refresh it */
        food_ = 1;
        return 0;
    } else {
        return 1;
    }
}

bool Dog::IsHealthy() const {
    return health_ == DogState::DOG_HEALTHY;
}

bool Dog::IsSick() const {
    return health_ == DogState::DOG_SICK;
}

bool Dog::IsDead() const {
    return health_ == DogState::DOG_DEAD;
}

void Dog::Revive() {
    if (health_ != DogState::DOG_DEAD)
        LOG(ERROR) << "Unexpected workflow, try to revive a living dog";

    health_ = DogState::DOG_HEALTHY;
}

void Dog::SetSick() {
    if (health_ == DogState::DOG_HEALTHY) health_ = DogState::DOG_SICK;
}

void Dog::SetDead() {
    health_ = DogState::DOG_DEAD;
}

bool Dog::IsDestroy() const {
    return destroy_;
}

void Dog::SetDestroy() {
    destroy_ = true;
    health_ = DogState::DOG_DEAD;
}

void Dog::OnError() {
    LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
        << "Error detected by watchdog, storDir=" << config_->storDir;
    SetDead();
    if (config_->watchdogKillOnErr) {
        SetDestroy();

        /* gracefully kill chunkserver */
        raise(SIGTERM);
        sleep(config_->watchdogKillTimeoutSec);
        /* forcibly kill chunkserver */
        raise(SIGKILL);
    }
}

}  // namespace chunkserver
}  // namespace curve
