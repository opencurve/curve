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

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
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
    * @brief Store key-value to the cache
    *
    * @param[in] key
    * @param[in] value
    */
    virtual void Put(const std::string &key, const std::string &value) = 0;

    /*
    * @brief Get corresponding value of the key from the cache
    *
    * @param[in] key
    * @param[out] value
    *
    * @return false if failed, true if succeeded
    */
    virtual bool Get(const std::string &key, std::string *value) = 0;

    /*
    * @brief Remove Remove key-value from cache
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
    * @brief PutLocked Store key-value in cache, not thread safe
    *
    * @param[in] key
    * @param[in] value
    */
    void PutLocked(const std::string &key, const std::string &value);

    /*
    * @brief RemoveLocked Remove key-value from the cache, not thread safe
    *
    * @param[in] key
    */
    void RemoveLocked(const std::string &key);

    /*
    * @brief MoveToFront Move the element hit this to the head of the list
    *
    * @param[in] elem Target element
    */
    void MoveToFront(const std::list<Item>::iterator &elem);

    /*
    * @brief RemoveOldest Remove elements exceeded maxCount
    */
    void RemoveOldest();

    /*
    * @brief RemoveElement Remove specified element
    *
    * @param[in] elem Specified element
    */
    void RemoveElement(const std::list<Item>::iterator &elem);

 private:
    ::curve::common::RWLock lock_;

    // the maximum length of the queue. 0 indicates unlimited length
    int maxCount_;
    // dequeue for storing items
    std::list<Item> ll_;
    // record the position of the item corresponding to the key in the dequeue
    std::unordered_map<std::string, std::list<Item>::iterator> cache_;

    // cache related metric data
    std::shared_ptr<NameserverCacheMetrics> cacheMetrics_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_CACHE_H_
