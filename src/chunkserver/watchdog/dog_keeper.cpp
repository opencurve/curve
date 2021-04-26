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

#include "src/chunkserver/watchdog/dog_keeper.h"

#include <glog/logging.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "src/chunkserver/watchdog/common.h"

namespace curve {
namespace chunkserver {

DECLARE_bool(watchdogKillOnErr);
DECLARE_bool(watchdogRevive);
DECLARE_uint32(watchdogCheckPeriodSec);
DECLARE_uint32(watchdogCheckTimeoutSec);
DECLARE_uint32(watchdogLatencyCheckPeriodSec);
DECLARE_uint32(watchdogLatencyCheckWindow);
DECLARE_uint32(watchdogLatencyExcessMs);
DECLARE_uint32(watchdogLatencyAbnormalMs);
DECLARE_uint32(watchdogLatencyWindowSlowRatio);
DECLARE_uint32(watchdogKillTimeoutSec);

void Keeper(DogKeeper* keeper) {
    LOG(INFO) << "keeper thread is running";
    while (true) {
        if (keeper->IsDestroy()) {
            LOG(INFO) << "dogkeeper thread destroyed";
            return;
        }

        keeper->UpdateConfig();

        if (keeper->IsDogDead()) {
            /* dog already dead, waiting for revive */
            if (FLAGS_watchdogRevive) {
                FLAGS_watchdogRevive = false;
                keeper->ReviveDog();
                LOG(INFO) << "watchdog revived, storDir="
                          << keeper->conf_.storDir;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kWatchdogSleepMs));
        } else {
            keeper->FeedDog();
            /* sleep 1 second */
            for (int i = 0; i < kWatchdogSleepFreq; ++i) {
                if (keeper->IsDestroy()) {
                    LOG(INFO) << "dogkeeper thread destroyed";
                    return;
                }
                keeper->CheckDog();
                if (keeper->IsDogDead()) break;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(kWatchdogSleepMs));
            }
        }
    }

    return;
}

int DogKeeper::Run() {
    if (dog_->IsDead() || dog_->IsSick()) {
        LOG(ERROR) << "invalid watchdog status, skip running";
        return -1;
    }

    UpdateConfig();
    if (dog_->Run() < 0) {
        LOG(ERROR) << "Failed to run watchdog";
        return -1;
    }

    thread_ = std::thread(Keeper, this);

    return 0;
}

int DogKeeper::Init() {
    if (dog_->Init(&conf_) < 0) {
        LOG(ERROR) << "Failed to init watchdog";
        return -1;
    }

    LOG(INFO) << "watchdog initialized";
    return 0;
}

void DogKeeper::FeedDog() {
    stallSec_ += dog_->Feed();
    if (stallSec_ >= conf_.watchdogCheckTimeoutSec) {
        LOG(ERROR) << "dog stall timeout, set to sick";
        dog_->SetSick();
    }
}

void DogKeeper::CheckDog() {
    if (dog_->IsSick()) dog_->OnError();
}

bool DogKeeper::IsDogDead() const {
    return dog_->IsDead();
}

void DogKeeper::ReviveDog() {
    dog_->Revive();
}

bool DogKeeper::IsDestroy() const {
    return destroy_;
}

void DogKeeper::SetDestroy() {
    destroy_ = true;
}

void DogKeeper::UpdateConfig() {
    WATCHDOG_UPDATE_CONFIG(conf_, watchdogCheckPeriodSec);
    WATCHDOG_UPDATE_CONFIG(conf_, watchdogCheckTimeoutSec);
    WATCHDOG_UPDATE_CONFIG(conf_, watchdogKillTimeoutSec);
    WATCHDOG_UPDATE_CONFIG(conf_, watchdogKillOnErr);
}

}  // namespace chunkserver
}  // namespace curve
