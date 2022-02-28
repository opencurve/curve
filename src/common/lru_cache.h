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
 * Created Date: 20211010
 * Author: xuchaojie, lixiaocui
 */

#ifndef SRC_COMMON_LRU_CACHE_H_
#define SRC_COMMON_LRU_CACHE_H_

#include <bvar/bvar.h>

#include <list>
#include <string>
#include <memory>
#include <unordered_map>
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace common {

class CacheMetrics {
 public:
    explicit CacheMetrics(const std::string &metricPrefix)
      : cacheCount(metricPrefix, "cache_count"),
        cacheBytes(metricPrefix, "cache_bytes"),
        cacheHit(metricPrefix, "cache_hit"),
        cacheMiss(metricPrefix, "cache_miss") {}

    void UpdateAddToCacheCount() {
        cacheCount << 1;
    }

    void UpdateRemoveFromCacheCount() {
        cacheCount << -1;
    }

    void UpdateAddToCacheBytes(uint64_t size) {
        cacheBytes << size;
    }

    void UpdateRemoveFromCacheBytes(uint64_t size) {
        cacheBytes << (0 - size);
    }

    void OnCacheHit() {
        cacheHit << 1;
    }

    void OnCacheMiss() {
        cacheMiss << 1;
    }

 public:
    bvar::Adder<uint32_t> cacheCount;
    bvar::Adder<uint64_t> cacheBytes;
    bvar::Adder<uint64_t> cacheHit;
    bvar::Adder<uint64_t> cacheMiss;
};

template<class T>
struct CacheTraits {
    static uint64_t CountBytes(const T &) {
        return sizeof(T);
    }
};

template<>
struct CacheTraits<std::string> {
    static uint64_t CountBytes(const std::string &v) {
        return v.size();
    }
};


template <typename K,  typename V>
class LRUCacheInterface {
 public:
    /**
     * @brief Store key-value to the cache
     *
     * @param[in] key
     * @param[in] value
     *
     */
    virtual void Put(const K &key, const V &value) = 0;

    /**
     * @brief Store key-value to the cache, and return the eliminated one
     *
     * @param[in] key
     * @param[in] value
     * @param[out] eliminated The value eliminated by the cache
     *
     * @return true if have eliminated item, false if not have
     */
    virtual bool Put(const K &key, const V &value, V *eliminated) = 0;

    /*
    * @brief Get corresponding value of the key from the cache
    *
    * @param[in] key
    * @param[out] value
    *
    * @return false if failed, true if succeeded
    */
    virtual bool Get(const K &key, V *value) = 0;

    /*
    * @brief Remove Remove key-value from cache
    *
    * @param[in] key
    */
    virtual void Remove(const K &key) = 0;
};


template <typename K,  typename V,
    typename KeyTraits = CacheTraits<K>,
    typename ValueTraits = CacheTraits<V>>
class LRUCache : public LRUCacheInterface<K, V> {
 public:
    struct Item {
        K key;
        V value;
    };

 public:
    explicit LRUCache(std::shared_ptr<CacheMetrics> cacheMetrics = nullptr)
      : maxCount_(0),
        cacheMetrics_(cacheMetrics) {}

    explicit LRUCache(uint64_t maxCount,
        std::shared_ptr<CacheMetrics> cacheMetrics = nullptr)
      : maxCount_(maxCount),
        cacheMetrics_(cacheMetrics) {}

    /**
     * @brief Store key-value to the cache
     *
     * @param[in] key
     * @param[in] value
     *
     */
    void Put(const K &key, const V &value) override;

    /**
     * @brief Store key-value to the cache, and return the eliminated one
     *
     * @param[in] key
     * @param[in] value
     * @param[out] eliminated The value eliminated by the cache
     *
     * @return true if have eliminated item, false if not have
     */
    bool Put(const K &key, const V &value, V *eliminated) override;

    /*
    * @brief Get corresponding value of the key from the cache
    *
    * @param[in] key
    * @param[out] value
    *
    * @return false if failed, true if succeeded
    */
    bool Get(const K &key, V *value) override;

    /*
    * @brief Remove Remove key-value from cache
    *
    * @param[in] key
    */
    void Remove(const K &key) override;

    std::shared_ptr<CacheMetrics> GetCacheMetrics() const;

 private:
    /*
    * @brief PutLocked Store key-value in cache, not thread safe
    *
    * @param[in] key
    * @param[in] value
    * @param[out] eliminated The value eliminated by the cache
    *
    * @return true if have eliminated item, false if not have
    */
    bool PutLocked(const K &key, const V &value, V *eliminated);

    /*
    * @brief RemoveLocked Remove key-value from the cache, not thread safe
    *
    * @param[in] key
    */
    void RemoveLocked(const K &key);

    /*
    * @brief MoveToFront Move the element hit this to the head of the list
    *
    * @param[in] elem Target element
    */
    void MoveToFront(const typename std::list<Item>::iterator &elem);

    /*
    * @brief RemoveOldest Remove elements exceeded maxCount
    *
    * @return The value eliminated by the cache
    */
    bool RemoveOldest(V *eliminated);

    /*
    * @brief RemoveElement Remove specified element
    *
    * @param[in] elem Specified element
    */
    void RemoveElement(const typename std::list<Item>::iterator &elem);

 private:
    ::curve::common::RWLock lock_;

    // the maximum length of the queue. 0 indicates unlimited length
    uint64_t maxCount_;
    // dequeue for storing items
    std::list<Item> ll_;
    // record the position of the item corresponding to the key in the dequeue
    std::unordered_map<K, typename std::list<Item>::iterator> cache_;

    // cache related metric data
    std::shared_ptr<CacheMetrics> cacheMetrics_;
};

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
void LRUCache<K, V, KeyTraits, ValueTraits>::Put(
    const K &key, const V &value) {
    V eliminated;
    ::curve::common::WriteLockGuard guard(lock_);
    PutLocked(key, value, &eliminated);
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
bool LRUCache<K, V, KeyTraits, ValueTraits>::Put(
    const K &key, const V &value, V *eliminated) {
    ::curve::common::WriteLockGuard guard(lock_);
    return PutLocked(key, value, eliminated);
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
bool LRUCache<K, V, KeyTraits, ValueTraits>::Get(const K &key, V *value) {
    ::curve::common::WriteLockGuard guard(lock_);
    auto iter = cache_.find(key);
    if (iter == cache_.end()) {
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->OnCacheMiss();
        }
        return false;
    }

    if (cacheMetrics_ != nullptr) {
        cacheMetrics_->OnCacheHit();
    }

    // update the position of the target item in the list
    MoveToFront(iter->second);
    *value = cache_[key]->value;
    return true;
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
void LRUCache<K, V, KeyTraits, ValueTraits>::Remove(const K &key) {
    ::curve::common::WriteLockGuard guard(lock_);
    RemoveLocked(key);
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
bool LRUCache<K, V, KeyTraits, ValueTraits>::PutLocked(
    const K &key, const V &value, V *eliminated) {
    auto iter = cache_.find(key);

    // delete the old value if already exist
    if (iter != cache_.end()) {
        RemoveElement(iter->second);
    }

    // put new value
    Item kv{key, value};
    ll_.push_front(kv);
    cache_[key] = ll_.begin();
    if (cacheMetrics_ != nullptr) {
        cacheMetrics_->UpdateAddToCacheCount();
        cacheMetrics_->UpdateAddToCacheBytes(
           KeyTraits::CountBytes(key)  + ValueTraits::CountBytes(value));
    }
    if (maxCount_ != 0 && ll_.size() > maxCount_) {
        return RemoveOldest(eliminated);
    }
    return false;
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
void LRUCache<K, V, KeyTraits, ValueTraits>::RemoveLocked(const K &key) {
    auto iter = cache_.find(key);
    if (iter != cache_.end()) {
        RemoveElement(iter->second);
    }
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
void LRUCache<K, V, KeyTraits, ValueTraits>::MoveToFront(
    const typename std::list<Item>::iterator &elem) {
    Item duplica{elem->key, elem->value};
    ll_.erase(elem);
    ll_.push_front(duplica);
    cache_[duplica.key] = ll_.begin();
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
bool LRUCache<K, V, KeyTraits, ValueTraits>::RemoveOldest(V *eliminated) {
    if (ll_.begin() != ll_.end()) {
        *eliminated = ll_.back().value;
        RemoveElement(--ll_.end());
    }
    return false;
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
void LRUCache<K, V, KeyTraits, ValueTraits>::RemoveElement(
    const typename std::list<Item>::iterator &elem) {
    if (cacheMetrics_ != nullptr) {
        cacheMetrics_->UpdateRemoveFromCacheCount();
        cacheMetrics_->UpdateRemoveFromCacheBytes(
            KeyTraits::CountBytes(elem->key) +
            ValueTraits::CountBytes(elem->value));
    }

    auto iter = cache_.find(elem->key);
    cache_.erase(iter);
    ll_.erase(elem);
}

template <typename K,  typename V, typename KeyTraits, typename ValueTraits>
std::shared_ptr<CacheMetrics>
    LRUCache<K, V, KeyTraits, ValueTraits>::GetCacheMetrics() const {
    return  cacheMetrics_;
}

}  // namespace common
}  // namespace curve


#endif  // SRC_COMMON_LRU_CACHE_H_
