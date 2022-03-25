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

#ifndef CURVEFS_SRC_METASERVER_ITERATOR_H_
#define CURVEFS_SRC_METASERVER_ITERATOR_H_

#include <string>

namespace curvefs {
namespace metaserver {

// The iterator can traverse different data struct: like hash, B+ tree, skiplist
class Iterator {
 public:
    virtual ~Iterator() = default;

    // TODO(Wine93): remove this interface
    virtual uint64_t Size() = 0;

    virtual bool Valid() = 0;

    virtual void SeekToFirst() = 0;

    virtual void Next() = 0;

    virtual std::string Key() = 0;

    virtual std::string Value() = 0;

    virtual int Status() = 0;
};

};  // namespace metaserver
};  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_ITERATOR_H_
