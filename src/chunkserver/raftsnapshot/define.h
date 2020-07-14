/*
 * Project: curve
 * Created Date: 2020-06-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_

namespace curve {
namespace chunkserver {

const char RAFT_DATA_DIR[] = "data";
const char RAFT_META_DIR[] = "raft_meta";

// TODO(all:fix it): RAFT_SNAP_DIR注意当前这个目录地址不能修改
// 与当前外部依赖curve-braft代码强耦合（两边硬编码耦合）
// 详见http://jira.netease.com/browse/CLDCFS-1937
const char RAFT_SNAP_DIR[] = "raft_snapshot";
const char RAFT_LOG_DIR[]  = "log";
#define BRAFT_SNAPSHOT_PATTERN "snapshot_%020" PRId64
#define BRAFT_SNAPSHOT_ATTACH_META_FILE "__raft_snapshot_attach_meta"

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_
