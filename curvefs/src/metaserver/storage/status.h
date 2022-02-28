/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: 2022-02-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_STATUS_H_
#define CURVEFS_SRC_METASERVER_STORAGE_STATUS_H_

#include <string>

namespace curvefs {
namespace metaserver {
namespace storage {

class Status {
 public:
    enum Code : unsigned char {
        kOk = 0,
        kDBClosed = 1,
        kNotFound = 2,
        kNotSupported = 3,
        kInternalError = 4,
        kSerializedFailed = 5,
        kParsedFailed = 6,
        kMaxCode
    };

 public:
    Status() : code_(kOk) {}
    ~Status() = default;

    static Status OK() { return Status(); }
    static Status DBClosed() { return Status(kDBClosed); }
    static Status NotFound() { return Status(kNotFound); }
    static Status NotSupported() { return Status(kNotSupported); }
    static Status InternalError() { return Status(kInternalError); }
    static Status SerializedFailed() { return Status(kSerializedFailed); }
    static Status ParsedFailed() { return Status(kParsedFailed); }

    bool ok() const { return code() == kOk; }
    bool IsDBClosed() const { return code() == kDBClosed; }
    bool IsNotFound() const { return code() == kNotFound; }
    bool IsNotSupported() const { return code() == kNotSupported; }
    bool IsInternalError() const { return code() == kInternalError; }
    bool IsSerializedFailed() const { return code() == kSerializedFailed; }
    bool IsParsedFailed() const {  return code() == kParsedFailed; }

    Code code() const { return code_; }
    std::string ToString() const;

 private:
    explicit Status(Code _code) : code_(_code) {}

 private:
    Code code_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_STATUS_H_
