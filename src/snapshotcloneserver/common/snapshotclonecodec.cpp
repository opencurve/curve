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

#include "src/snapshotcloneserver/common/snapshotclonecodec.h"

namespace curve {
namespace snapshotcloneserver {

std::string SnapshotCloneCodec::EncodeSnapshotKey(
    const std::string &uuid) {
    std::string key = SnapshotCloneCodec::GetSnapshotInfoKeyPrefix();
    key += uuid;
    return key;
}

bool SnapshotCloneCodec::EncodeSnapshotData(
    const SnapshotInfo &data, std::string *value) {
    return data.SerializeToString(value);
}

bool SnapshotCloneCodec::DecodeSnapshotData(
    const std::string &value, SnapshotInfo *data) {
    return data->ParseFromString(value);
}

std::string SnapshotCloneCodec::EncodeCloneInfoKey(
    const std::string &uuid) {
    std::string key = SnapshotCloneCodec::GetCloneInfoKeyPrefix();
    key += uuid;
    return key;
}

bool SnapshotCloneCodec::EncodeCloneInfoData(
    const CloneInfo &data, std::string *value) {
    return data.SerializeToString(value);
}

bool SnapshotCloneCodec::DecodeCloneInfoData(
    const std::string &value, CloneInfo *data) {
    return data->ParseFromString(value);
}

}  // namespace snapshotcloneserver
}  // namespace curve

