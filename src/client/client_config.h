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
 * File Created: Tuesday, 23rd October 2018 4:46:29 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_CLIENT_CONFIG_H_
#define SRC_CLIENT_CLIENT_CONFIG_H_

#include "src/client/config_info.h"
#include "src/common/configuration.h"

namespace curve {
namespace client {

class ClientConfig {
 public:
    int Init(const char* configpath);

    FileServiceOption GetFileServiceOption() const {
        return fileServiceOption_;
    }

    /**
     * test use, set the fileServiceOption_
     */
    void SetFileServiceOption(FileServiceOption opt) {
        fileServiceOption_ = opt;
    }

    uint16_t GetDummyserverStartPort();

 private:
    FileServiceOption fileServiceOption_;
    common::Configuration conf_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_CLIENT_CONFIG_H_
