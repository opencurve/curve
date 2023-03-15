/*
 *  Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2021-07-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_ITERATOR_H_
#define CURVEFS_SRC_METASERVER_STORAGE_ITERATOR_H_

#include <string>
#include <vector>
#include <memory>

#include "curvefs/src/metaserver/storage/common.h"

namespace curvefs {
namespace metaserver {
namespace storage {

// The iterator can traverse different data struct: like hash, B+ tree, skiplist
class Iterator {
 public:
    virtual ~Iterator() = default;

    virtual uint64_t Size() = 0;

    virtual bool Valid() = 0;

    virtual void SeekToFirst() = 0;

    virtual void Next() = 0;

    virtual std::string Key() = 0;

    virtual std::string Value() = 0;

    virtual const ValueType *RawValue() const { return nullptr; }

    virtual int Status() = 0;

    virtual bool ParseFromValue(ValueType * /*value*/) { return true; }

    virtual void DisablePrefixChecking() {}
};

class MergeIterator : public Iterator {
 public:
    using ChildrenType = std::vector<std::shared_ptr<Iterator>>;

 public:
    explicit MergeIterator(const ChildrenType &children)
        : children_(children), current_(nullptr) {}

    uint64_t Size() override {
        uint64_t size = 0;
        for (const auto &child : children_) {
            size += child->Size();
        }

        return size;
    }

    bool Valid() override { return (current_ != nullptr) && (Status() == 0); }

    void SeekToFirst() override {
        for (const auto &child : children_) {
            child->SeekToFirst();
        }
        FindCurrent();
    }

    void Next() override {
        current_->Next();
        FindCurrent();
    }

    std::string Key() override { return current_->Key(); }

    std::string Value() override { return current_->Value(); }

    int Status() override {
        for (const auto &child : children_) {
            if (child->Status() != 0) {
                return child->Status();
            }
        }
        return 0;
    }

 private:
    void FindCurrent() {
        current_ = nullptr;
        for (const auto &child : children_) {
            if (child->Valid()) {
                current_ = child;
                break;
            }
        }
    }

 private:
    ChildrenType children_;
    std::shared_ptr<Iterator> current_;
};

template <typename ContainerType> class ContainerIterator : public Iterator {
 public:
    explicit ContainerIterator(std::shared_ptr<ContainerType> container)
        : container_(container) {}

    uint64_t Size() override { return container_->size(); }

    bool Valid() override { return iter_ != container_->end(); }

    void SeekToFirst() override { iter_ = container_->begin(); }

    void Next() override { iter_++; }

    std::string Key() override { return iter_->first; }

    std::string Value() override { return iter_->second; }

    int Status() override { return 0; }

 protected:
    const std::shared_ptr<ContainerType> container_;
    typename ContainerType::const_iterator iter_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_ITERATOR_H_
