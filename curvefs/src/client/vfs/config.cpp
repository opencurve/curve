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

/*
 * Project: Curve
 * Created Date: 2023-07-11
 * Author: Jingli Chen (Wine93)
 */

#include <vector>
#include <string>

#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"
#include "curvefs/src/client/vfs/utils.h"
#include "curvefs/src/client/vfs/config.h"

#define INCBIN_STYLE INCBIN_STYLE_SNAKE
#define INCBIN_PREFIX g_
#include <incbin.h>
/* Usage: INCBIN(<<LABLE>>, <<FILE>>)
 *
 * Symbols defined by INCBIN
 * ------------------------------------------
 *  const unsigned char        g_client_conf_data[]  // g_<<LABLE>>_data
 *  const unsigned char* const g_client_conf_end;    // g_<<LABEL>>_end
 *  const unsinged int         g_client_conf_size;   // g_<<LABEL>>_size
 */

#ifndef CLIENT_CONF_PATH
#define CLIENT_CONF_PATH "/curve/curvefs/conf/client.conf"
#endif

INCBIN(client_conf, CLIENT_CONF_PATH);

namespace curvefs {
namespace client {
namespace vfs {

std::shared_ptr<Configure> Configure::Default() {
    auto cfg = std::make_shared<Configure>();
    cfg->LoadString(reinterpret_cast<const char*>(g_client_conf_data));
    return cfg;
}

void Configure::LoadLine(const std::string& l) {
    std::string line = strings::TrimSpace(l);
    if (strings::HasPrefix(line, "#") || line.empty()) {
        return;
    }

    // key
    int idx1 = line.find("=");
    std::string key = line.substr(0, idx1);
    key = strings::TrimSpace(key);

    // value
    int idx2 = line.find("#");
    std::string value = line.substr(idx1 + 1, idx2 - idx1 - 1);
    value = strings::TrimSpace(value);

    m_[key] = value;
}

void Configure::LoadString(const std::string& in) {
    std::vector<std::string> lines = strings::Split(in, "\n");
    for (const auto& line : lines) {
        LoadLine(line);
    }
}

bool Configure::Get(const std::string& key, std::string* value) {
    auto iter = m_.find(key);
    if (iter == m_.end()) {
        return false;
    }
    *value = iter->second;
    return true;
}

void Configure::Set(const std::string& key, const std::string& value) {
    m_[key] = value;
}

void Configure::Iterate(IterateHandler handler) {
    for (const auto& item : m_) {
        handler(item.first, item.second);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
