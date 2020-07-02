/*
 * Project: nebd
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2019 netease
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
