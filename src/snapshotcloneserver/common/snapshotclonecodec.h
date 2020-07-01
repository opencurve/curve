/*
 * Project: curve
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 * Copyright (c) 2020 netease
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
