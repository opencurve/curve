/*
 * Project: curve
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 * Copyright (c) 2020 netease
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

