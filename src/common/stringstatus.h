/*
 * Project: curve
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */


#ifndef  SRC_COMMON_STRINGSTATUS_H_
#define  SRC_COMMON_STRINGSTATUS_H_

#include <bvar/bvar.h>
#include <string>
#include <map>

namespace curve {
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
}  // namespace curve

#endif  // SRC_COMMON_STRINGSTATUS_H_

