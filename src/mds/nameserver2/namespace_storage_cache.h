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
 * Created Date: Thur Apr 16th 2019
 * Author: hzlixiaocui
 */

#ifndef SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_CACHE_H_
#define SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_CACHE_H_

#include <string>
#include <list>
#include <map>
#include <memory>
#include "src/common/concurrent/concurrent.h"
#include "src/mds/nameserver2/nameserverMetrics.h"

namespace curve {
namespace mds {
struct Item {
    std::string key;
    std::string value;
};

class Cache {
 public:
    /*
    * @brief Put 存储key-value到缓存
    *
    * @param[in] key
    * @param[in] value
    */
    virtual void Put(const std::string &key, const std::string &value) = 0;

    /*
    * @brief Get 从缓存中获取key对应的value
    *
    * @param[in] key
    * @param[out] value
    *
    * @return false-获取不到 true-获取成功
    */
    virtual bool Get(const std::string &key, std::string *value) = 0;

    /*
    * @brief Remove 从缓存中移除key-value
    *
    * @param[in] key
    */
    virtual void Remove(const std::string &key) = 0;
};

class LRUCache : public Cache {
 public:
    LRUCache() : maxCount_(0) {
        cacheMetrics_ = std::make_shared<NameserverCacheMetrics>();
    }
    explicit LRUCache(int maxCount) : maxCount_(maxCount) {
        cacheMetrics_ = std::make_shared<NameserverCacheMetrics>();
    }

    void Put(const std::string &key, const std::string &value) override;
    bool Get(const std::string &key, std::string *value) override;
    void Remove(const std::string &key) override;
    std::shared_ptr<NameserverCacheMetrics> GetCacheMetrics() const;

 private:
    /*
    * @brief PutLocked 存储key-value到缓存，非线程安全
    *
    * @param[in] key
    * @param[in] value
    */
    void PutLocked(const std::string &key, const std::string &value);

    /*
    * @brief RemoveLocked 从缓存中移除key-value，非线程安全
    *
    * @param[in] key
    */
    void RemoveLocked(const std::string &key);

    /*
    * @brief MoveToFront 把本次击中的元素移动到list的头部
    *
    * @param[in] elem 本次击中的元素
    */
    void MoveToFront(const std::list<Item>::iterator &elem);

    /*
    * @brief RemoveOldest 移除超过maxCount的元素
    */
    void RemoveOldest();

    /*
    * @brief RemoveElement 移除指定元素
    *
    * @param[in] elem 指定元素
    */
    void RemoveElement(const std::list<Item>::iterator &elem);

 private:
    ::curve::common::RWLock lock_;

    // 队列的最大长度. 为0表示长度不限
    int maxCount_;
    // 存储Item的双向队列
    std::list<Item> ll_;
    // 记录key对应的Item在双向列表中的位置
    std::map<std::string, std::list<Item>::iterator> cache_;

    // cache相关metric统计
    std::shared_ptr<NameserverCacheMetrics> cacheMetrics_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_CACHE_H_
