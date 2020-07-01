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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONECODEC_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONECODEC_H_

#include <string>

#include "src/snapshotcloneserver/common/snapshotclone_info.h"
#include "src/common/namespace_define.h"

using ::curve::common::SNAPINFOKEYPREFIX;
using ::curve::common::SNAPINFOKEYEND;
using ::curve::common::CLONEINFOKEYPREFIX;
using ::curve::common::CLONEINFOKEYEND;

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneCodec {
 public:
    std::string EncodeSnapshotKey(const std::string &uuid);
    bool EncodeSnapshotData(const SnapshotInfo &data, std::string *value);
    bool DecodeSnapshotData(const std::string &value, SnapshotInfo *data);

    std::string EncodeCloneInfoKey(const std::string &uuid);
    bool EncodeCloneInfoData(const CloneInfo &data, std::string *value);
    bool DecodeCloneInfoData(const std::string &value, CloneInfo *data);

    static std::string GetSnapshotInfoKeyPrefix() {
        return std::string(SNAPINFOKEYPREFIX);
    }

    static std::string GetSnapshotInfoKeyEnd() {
        return std::string(SNAPINFOKEYEND);
    }

    static std::string GetCloneInfoKeyPrefix() {
        return std::string(CLONEINFOKEYPREFIX);
    }

    static std::string GetCloneInfoKeyEnd() {
        return std::string(CLONEINFOKEYEND);
    }
};

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONECODEC_H_
