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
 * Created Date: 20190819
 * Author: lixiaocui
 */

#ifndef SRC_COMMON_STRINGSTATUS_H_
#define SRC_COMMON_STRINGSTATUS_H_

#include <bvar/bvar.h>

#include <map>
#include <string>

namespace curve {
namespace common {
class StringStatus {
 public:
    /**
     * @brief ExposeAs is used to initialize bvar
     *
     * @param[in] prefix, prefix
     * @param[in] name, first name
     */
    void ExposeAs(const std::string& prefix, const std::string& name);

    /**
     * @brief Set sets the key-value information for each item
     *
     * @param[in] key
     * @param[in] value
     */
    void Set(const std::string& key, const std::string& value);

    /**
     * @brief Update sets the key-value pairs in the current key-value map to
     * status as JSON strings// NOLINT
     */
    void Update();

    /**
     * @brief GetValueByKey Get the value corresponding to the specified key
     *
     * @param[in] key Specify the key
     */
    std::string GetValueByKey(const std::string& key);

    /**
     * @brief JsonBody obtains the JSON format string corresponding to the
     * current key-value map
     */
    std::string JsonBody();

 private:
    // The key-value map of the structure to be exported
    std::map<std::string, std::string> kvs_;
    // The status corresponding to the exported item
    bvar::Status<std::string> status_;
};
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_STRINGSTATUS_H_
