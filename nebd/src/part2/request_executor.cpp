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
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/request_executor.h"
#include "nebd/src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

NebdRequestExecutor* g_test_executor = nullptr;

NebdRequestExecutor*
NebdRequestExecutorFactory::GetExecutor(NebdFileType type) {
    NebdRequestExecutor* executor = nullptr;
    switch (type) {
        case NebdFileType::CURVE:
            executor = &CurveRequestExecutor::GetInstance();
            break;
        case NebdFileType::TEST:
            executor = g_test_executor;
            break;
        default:
            break;
    }
    return executor;
}

NebdFileInstancePtr
NebdFileInstanceFactory::GetInstance(NebdFileType type) {
    NebdFileInstancePtr instance = nullptr;
    switch (type) {
        case NebdFileType::CURVE:
            instance = std::make_shared<CurveFileInstance>();
            break;
        default:
            break;
    }
    return instance;
}

}  // namespace server
}  // namespace nebd
