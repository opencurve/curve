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


#ifndef  NEBD_SRC_COMMON_STRINGSTATUS_H_
#define  NEBD_SRC_COMMON_STRINGSTATUS_H_

#include <bvar/bvar.h>
#include <string>
#include <map>

namespace nebd {
namespace common {
class StringStatus {
 public:
    /**
     * @brief ExposeAs 用于初始化bvar
     *
     * @param[in] prefix, 前缀
     * @param[in] name, 名字
     */
    void ExposeAs(const std::string &prefix, const std::string &name);

    /**
     * @brief Set 设置每项key-value信息
     *
     * @param[in] key
     * @param[in] value
     */
    void Set(const std::string& key, const std::string& value);

    /**
     * @brief Update 把当前key-value map中的键值对以json string的形式设置到status中 //NOLINT
     */
    void Update();

    /**
     * @brief GetValueByKey 获取指定key对应的value
     *
     * @param[in] key 指定key
     */
    std::string GetValueByKey(const std::string &key);

    /**
     * @brief JsonBody 获取当前key-value map对应的json形式字符串
     */
    std::string JsonBody();

 private:
    // 需要导出的结构体的key-value map
    std::map<std::string, std::string> kvs_;
    // 该导出项对应的status
    bvar::Status<std::string> status_;
};
}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_STRINGSTATUS_H_
