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
 * Created Date: 20190819
 * Author: lixiaocui
 */

#include "nebd/src/common/stringstatus.h"

namespace nebd {
namespace common {
void StringStatus::ExposeAs(
    const std::string &prefix, const std::string &name) {
    status_.expose_as(prefix, name);
}

void StringStatus::Set(const std::string& key, const std::string& value) {
    kvs_[key] = value;
}

void StringStatus::Update() {
    if (kvs_.empty()) {
        return;
    }

    std::string jsonStr = "{";
    int count = 0;
    for (auto &item : kvs_) {
        count += 1;
        if (count == kvs_.size()) {
            jsonStr +=
                "\"" + item.first + "\"" + ":" + "\"" + item.second + "\"";
        } else {
            jsonStr += "\"" + item.first + "\"" + ":" + "\"" + item.second +
                "\"" + ",";
        }
    }

    jsonStr += "}";
    status_.set_value(jsonStr);
}

std::string StringStatus::JsonBody() {
    return status_.get_value();
}

std::string StringStatus::GetValueByKey(const std::string &key) {
    return kvs_[key];
}
}  // namespace common
}  // namespace nebd
