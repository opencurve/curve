/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_

#include <string>

namespace curve {
namespace snapshotserver {

typedef std::string UUID;

const uint64_t kChunkSplitSize = 1024u * 1024u;

const int kErrCodeSnapshotServerSuccess = 0;
const int kErrCodeSnapshotServerFail = -1;
const int kErrCodeSnapshotInternalError = -2;
const int kErrCodeSnapshotServiceIsStop = -3;
const int kErrCodeSnapshotTaskExist = -4;


}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_DEFINE_H_
