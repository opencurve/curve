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

#ifndef SRC_COMMON_ARC_CACHE_H_
#define SRC_COMMON_ARC_CACHE_H_

/*
 * Project: curve
 * Created Date: 20231018
 * Author: xuyifeng
 */

#include <assert.h>
#include <stdint.h>

#include <algorithm>
#include <list>
#include <memory>
#include <unordered_map>
#include <utility>

// Adaptive Replacement Cache (ARC)
template <typename K, typename V, typename KeyTraits = CacheTraits<K>,
          typename ValueTraits = CacheTraits<V>>
class ARCCache : public LRUCacheInterface<K, V> {
 public:
    struct ArcSizeInfo {
        uint64_t b1, t1;
        uint64_t b2, t2;

        uint64_t BSize() const { return b1 + b2; }
        uint64_t TSize() const { return t1 + t2; }
    };

    ARCCache(uint64_t max_count,
             std::shared_ptr<CacheMetrics> cacheMetrics = nullptr)
        : c_(max_count), p_(0), cacheMetrics_(cacheMetrics) {}

    void Put(const K& key, const V& value);
    bool Put(const K& key, const V& value, V* eliminated);
    bool Get(const K& key, V* value);
    void Remove(const K& key);
    uint64_t Size();
    std::shared_ptr<CacheMetrics> GetCacheMetrics() const;

    bool GetLast(const V value, K* key);
    bool GetLast(K* key, V* value);
    bool GetLast(K* key, V* value, bool (*f)(const V& value));
    uint64_t Capacity() const { return c_; }
    ArcSizeInfo ArcSize() const;

 private:
    ARCCache(const ARCCache&) = delete;
    void operator=(const ARCCache&) = delete;

    struct BMapVal;
    struct TMapVal;
    struct BListVal;
    struct TListVal;

    typedef std::unordered_map<K, BMapVal> BMap;
    typedef typename BMap::iterator bmap_iter;
    typedef std::unordered_map<K, TMapVal> TMap;
    typedef typename TMap::iterator tmap_iter;

    typedef typename std::list<BListVal> BList;
    typedef typename BList::iterator blist_iter;
    typedef typename std::list<TListVal> TList;
    typedef typename TList::iterator tlist_iter;

    struct BMapVal {
        blist_iter list_iter;

        BMapVal() = default;
        BMapVal(const BMapVal& o) = default;
        BMapVal& operator=(const BMapVal& o) = default;
        BMapVal(const blist_iter& iter)  // NOLINT
            : list_iter(iter) {}
    };
    struct TMapVal {
        tlist_iter list_iter;

        TMapVal() = default;
        TMapVal(const TMapVal& o) = default;
        TMapVal& operator=(const TMapVal& o) = default;
        TMapVal(const tlist_iter& iter)  // NOLINT
            : list_iter(iter) {}
    };
    struct BListVal {
        bmap_iter map_iter;

        BListVal() = default;
        BListVal(const BListVal& o) = default;
        BListVal& operator=(const BListVal& o) = default;
        BListVal(const bmap_iter& iter)  // NOLINT
            :map_iter(iter) {}
    };
    struct TListVal {
        tmap_iter map_iter;
        V value;

        TListVal() = default;
        TListVal(const TListVal& o) = default;
        TListVal(const tmap_iter& iter, const V& v)
            : map_iter(iter), value(v) {}
        explicit TListVal(const V& v)
            : value(v) {}
        TListVal& operator=(const TListVal& o) = default;
    };

    struct B {
        BMap map_;
        BList list_;

        bool Find(const K& k, bmap_iter* iter) {
            auto it = map_.find(k);
            if (it != map_.end()) {
                if (iter != nullptr) *iter = it;
                return true;
            }
            return false;
        }

        void Insert(const K& k) {
            auto r = map_.insert({k, blist_iter()});
            assert(r.second);
            list_.push_back(r.first);
            r.first->second.list_iter = --list_.end();
        }

        void RemoveLRU(CacheMetrics* m) {
            if (list_.empty()) return;
            if (m) {
                m->UpdateRemoveFromCacheBytes(
                    KeyTraits::CountBytes(list_.front().map_iter->first));
            }
            map_.erase(list_.front().map_iter);
            list_.pop_front();
        }

        void Remove(bmap_iter&& map_iter, CacheMetrics* m) {
            if (m) {
                m->UpdateRemoveFromCacheBytes(
                    KeyTraits::CountBytes(map_iter->first));
            }
            list_.erase(map_iter->second.list_iter);
            map_.erase(map_iter);
        }

        uint64_t Count() const { return map_.size(); }
    };

    struct T {
        TMap map_;
        TList list_;

        void Insert(const K& k, const V& v, CacheMetrics* m) {
            auto r = map_.insert({k, tlist_iter()});
            assert(r.second);
            list_.emplace_back(r.first, v);
            r.first->second.list_iter = --list_.end();
            if (m != nullptr) {
                m->UpdateAddToCacheCount();
                m->UpdateAddToCacheBytes(KeyTraits::CountBytes(k) +
                                         ValueTraits::CountBytes(v));
            }
        }

        // Move key-value from another to this. For C++17 and later,
        // we use extract() and insert() to improve performance.
        void Move(const K& k, T* other, tmap_iter&& other_it, const V *v,
                CacheMetrics *m) {
            list_.splice(list_.end(), other->list_, other_it->second.list_iter);
            if (v != nullptr) {
                if (m != nullptr) {
                    uint64_t oldSize =
                        ValueTraits::CountBytes(list_.back().value);
                    uint64_t newSize = ValueTraits::CountBytes(*v);
                    if (oldSize != newSize) {
                        if (oldSize != 0)
                            m->UpdateRemoveFromCacheBytes(oldSize);
                        m->UpdateAddToCacheBytes(newSize);
                    }
                }
                list_.back().value = *v;
            }
#if __cplusplus >= 201703L
            auto node = other->map_.extract(other_it);
            auto r = map_.insert(std::move(node));
            assert(r.inserted);
            list_.back().map_iter = r.position;
            r.position->second.list_iter = --list_.end();
#else
            auto r = map_.insert({k, tlist_iter()});
            assert(r.second);
            list_.back().map_iter = r.first;
            r.first->second.list_iter = --list_.end();
            other->map_.erase(other_it);
#endif
        }

        bool Find(const K& k, tmap_iter* map_iter) {
            auto it = map_.find(k);
            if (it != map_.end()) {
                if (map_iter != nullptr) *map_iter = it;
                return true;
            }

            return false;
        }

        void Remove(tmap_iter&& map_iter, CacheMetrics* m) {
            if (m != nullptr) {
                m->UpdateRemoveFromCacheCount();
                m->UpdateRemoveFromCacheBytes(
                    KeyTraits::CountBytes(map_iter->first) +
                    ValueTraits::CountBytes(map_iter->second.list_iter->value));
            }
            list_.erase(map_iter->second.list_iter);
            map_.erase(map_iter);
        }

        bool RemoveLRU(V* eliminated, CacheMetrics* m) {
            if (list_.empty()) return false;
            if (eliminated != nullptr) {
                *eliminated = list_.front().value;
            }
            if (m != nullptr) {
                m->UpdateRemoveFromCacheCount();
                m->UpdateRemoveFromCacheBytes(
                    KeyTraits::CountBytes(list_.front().map_iter->first) +
                    ValueTraits::CountBytes(list_.front().value));
            }
            map_.erase(list_.front().map_iter);
            list_.pop_front();
            return true;
        }

        void Touch(const tmap_iter& map_iter, const V* v, CacheMetrics* m) {
            auto list_iter = --list_.end();
            if (v && m) {
                uint64_t oldSize =
                    ValueTraits::CountBytes(map_iter->second.list_iter->value);
                uint64_t newSize = ValueTraits::CountBytes(*v);
                if (oldSize != newSize) {
                    m->UpdateRemoveFromCacheBytes(oldSize);
                    m->UpdateAddToCacheBytes(newSize);
                }
            }
            if (list_iter == map_iter->second.list_iter) {
                if (v != nullptr) list_iter->value = *v;
                return;
            }

            list_.splice(list_.end(), list_, map_iter->second.list_iter);
            if (v != nullptr) {
                list_.back().value = *v;
            }
            map_iter->second.list_iter = --list_.end();
        }

        uint64_t Count() const { return map_.size(); }

        tmap_iter GetLRU() const { return list_.begin()->map_iter; }
    };

    bool Move_T_B(T* t, B* b, V* eliminated) {
        // move t's LRU item to b as MRU item
        if (t->Count() == 0) return false;

        auto map_iter = t->GetLRU();
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->UpdateRemoveFromCacheCount();
            // Key is not removed, only value is.
            cacheMetrics_->UpdateRemoveFromCacheBytes(
                ValueTraits::CountBytes(map_iter->second.list_iter->value));
        }
        if (eliminated) {
            *eliminated = map_iter->second.list_iter->value;
        }
        b->Insert(map_iter->first);
        t->Remove(std::move(map_iter), nullptr);
        return true;
    }

    bool IsCacheFull() const {
        return t1_.Count() + t2_.Count() == c_;
    }

    void IncreaseP(uint64_t delta) {
        if (!IsCacheFull())
            return;
        if (delta > c_ - p_)
            p_ = c_;
        else
            p_ += delta;
    }

    void DecreaseP(uint64_t delta) {
        if (!IsCacheFull())
            return;
        if (delta > p_)
            p_ = 0;
        else
            p_ -= delta;
    }

    bool Replace(const K& k, V* eliminated);

    mutable ::curve::common::RWLock lock_;
    uint64_t c_;
    uint64_t p_;
    B b1_, b2_;
    T t1_, t2_;
    std::shared_ptr<CacheMetrics> cacheMetrics_;
};

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::Get(const K& key, V* value) {
    ::curve::common::WriteLockGuard guard(lock_);
    tmap_iter it;

    if (t1_.Find(key, &it)) {
        if (value) *value = it->second.list_iter->value;
        t2_.Move(key, &t1_, std::move(it), nullptr, cacheMetrics_.get());
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->OnCacheHit();
        }
        return true;
    }
    if (t2_.Find(key, &it)) {
        if (value) *value = it->second.list_iter->value;
        t2_.Touch(it, nullptr, nullptr);
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->OnCacheHit();
        }
        return true;
    }
    if (cacheMetrics_ != nullptr) {
        cacheMetrics_->OnCacheMiss();
    }
    return false;
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
void ARCCache<K, V, KeyTraits, ValueTraits>::Put(const K& key, const V& value) {
    Put(key, value, nullptr);
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::Put(const K& key, const V& value,
                                                 V* eliminated) {
    ::curve::common::WriteLockGuard guard(lock_);
    tmap_iter it;
    bool ret = false;

    if (t1_.Find(key, &it)) {
        t2_.Move(key, &t1_, std::move(it), &value, cacheMetrics_.get());
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->OnCacheHit();
        }
        return false;
    }
    if (t2_.Find(key, &it)) {
        t2_.Touch(it, &value, cacheMetrics_.get());
        if (cacheMetrics_ != nullptr) {
            cacheMetrics_->OnCacheHit();
        }
        return false;
    }

    bmap_iter b_it;
    if (b1_.Find(key, &b_it)) {
        uint64_t delta = std::min((uint64_t)1, b2_.Count() / b1_.Count());
        IncreaseP(delta);

        ret = Replace(key, eliminated);
        b1_.Remove(std::move(b_it), cacheMetrics_.get());
        t2_.Insert(key, value, cacheMetrics_.get());
        return ret;
    }

    if (b2_.Find(key, &b_it)) {
        uint64_t delta = std::max((uint64_t)1, b1_.Count() / b2_.Count());
        DecreaseP(delta);

        ret = Replace(key, eliminated);
        b2_.Remove(std::move(b_it), cacheMetrics_.get());
        t2_.Insert(key, value, cacheMetrics_.get());
        return ret;
    }

    if (IsCacheFull() && t1_.Count() + b1_.Count() == c_) {
        if (t1_.Count() < c_) {
            b1_.RemoveLRU(cacheMetrics_.get());
            ret = Replace(key, eliminated);
        } else {
            ret = t1_.RemoveLRU(eliminated, cacheMetrics_.get());
        }
    } else if (t1_.Count() + b1_.Count() < c_) {
        auto total = t1_.Count() + b1_.Count() + t2_.Count() + b2_.Count();
        if (total >= c_) {
            if (total == 2 * c_) {
                if (b2_.Count() > 0) {
                    b2_.RemoveLRU(cacheMetrics_.get());
                } else {
                    b1_.RemoveLRU(cacheMetrics_.get());
                }
            }
            ret = Replace(key, eliminated);
        }
    }
    t1_.Insert(key, value, cacheMetrics_.get());
    return ret;
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::Replace(const K& k,
                                                     V* eliminated) {
    if (!IsCacheFull()) {
        return false;
    }
    if (t1_.Count() != 0 &&
        ((t1_.Count() > p_) || (b2_.Find(k, nullptr) && t1_.Count() == p_))) {
        return Move_T_B(&t1_, &b1_, eliminated);
    } else if (t2_.Count() > 0) {
        return Move_T_B(&t2_, &b2_, eliminated);
    } else {
        return Move_T_B(&t1_, &b1_, eliminated);
    }
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
uint64_t ARCCache<K, V, KeyTraits, ValueTraits>::Size() {
    ::curve::common::ReadLockGuard guard(lock_);
    return t1_.Count() + t2_.Count();
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
typename ARCCache<K, V, KeyTraits, ValueTraits>::ArcSizeInfo
ARCCache<K, V, KeyTraits, ValueTraits>::ArcSize() const {
    ::curve::common::ReadLockGuard guard(lock_);

    return {b1_.Count(), t1_.Count(),
            b2_.Count(), t2_.Count()};
}

// This operation detach the key from cache
template <typename K, typename V, typename KeyTraits, typename ValueTraits>
void ARCCache<K, V, KeyTraits, ValueTraits>::Remove(const K& key) {
    ::curve::common::WriteLockGuard guard(lock_);
    T* ts[]{&t1_, &t2_};
    for (auto t : ts) {
        tmap_iter it;
        if (t->Find(key, &it)) {
            t->Remove(std::move(it), cacheMetrics_.get());
            return;
        }
    }
    B* bs[]{&b1_, &b2_};
    for (auto b : bs) {
        bmap_iter it;
        if (b->Find(key, &it)) {
            b->Remove(std::move(it), cacheMetrics_.get());
            return;
        }
    }
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
std::shared_ptr<CacheMetrics>
ARCCache<K, V, KeyTraits, ValueTraits>::GetCacheMetrics() const {
    return cacheMetrics_;
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::GetLast(const V value, K* key) {
    // Note! this is not very precise, since ARC is not strictly
    // time ordered LRU
    ::curve::common::ReadLockGuard guard(lock_);
    T* ts[]{&t1_, &t2_};
    for (auto t : ts) {
        for (const auto& item : t->list_) {
            if (item.value == value) {
                *key = item.map_iter->first;
                return true;
            }
        }
    }
    return false;
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::GetLast(K* key, V* value) {
    ::curve::common::ReadLockGuard guard(lock_);

    T* ts[]{&t1_, &t2_};
    for (auto t : ts) {
        if (!t->list_.empty()) {
            *key = t->list_.front().map_iter->first;
            *value = t->list_.front().value;
            return true;
        }
    }

    return false;
}

template <typename K, typename V, typename KeyTraits, typename ValueTraits>
bool ARCCache<K, V, KeyTraits, ValueTraits>::GetLast(
    K* key, V* value, bool (*f)(const V& value)) {
    ::curve::common::ReadLockGuard guard(lock_);

    T* ts[]{&t1_, &t2_};
    for (auto t : ts) {
        for (const auto& item : t->list_) {
            bool ok = f(item.value);
            if (ok) {
                *key = item.map_iter->first;
                *value = item.value;
                return true;
            }
        }
    }
    return false;
}

#endif  // SRC_COMMON_ARC_CACHE_H_
