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

#include "curvefs/src/client/rpcclient/fsdelta_updater.h"

namespace curvefs {
namespace client {

void FsDeltaUpdater::UpdateDeltaBytes(int64_t deltaBytes) {
    deltaBytes_.fetch_add(deltaBytes);
}

int64_t FsDeltaUpdater::GetDeltaBytesAndReset() {
    return deltaBytes_.exchange(0);
}

}  // namespace client
}  // namespace curvefs
