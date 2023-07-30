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
 * Created Date: 2023-07-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_METRIC_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_METRIC_H_

#include <bvar/bvar.h>

#include <string>

namespace curvefs {
namespace client {
namespace filesystem {

class DirCacheMetric {
 public:
    DirCacheMetric() = default;

    void AddEntries(int64_t n) {
        metric_.nentries << n;
    }

 private:
    struct Metric {
        Metric() : nentries("filesystem_dircache", "nentries") {}
        bvar::Adder<int64_t> nentries;
    };

    Metric metric_;
};

class OpenfilesMetric {
 public:
    OpenfilesMetric() = default;

    void AddOpenfiles(int64_t n) {
        metric_.nfiles << n;
    }

 private:
    struct Metric {
        Metric() : nfiles("filesystem_openfiles", "nfiles") {}
        bvar::Adder<int64_t> nfiles;
    };

    Metric metric_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_METRIC_H_
