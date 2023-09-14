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
 * Created Date: 2020-06-10
 * Author: charisu
 */

#ifndef SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_
#define SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_

namespace curve {
namespace chunkserver {

const char RAFT_DATA_DIR[] = "data";
const char RAFT_META_DIR[] = "raft_meta";

// TODO(all:fix it): Note that the RAFT_SNAP_DIR directory address should not be modified at this time. 
// This is tightly coupled with the current external dependency on curve-braft code (hardcoded coupling on both sides).
const char RAFT_SNAP_DIR[] = "raft_snapshot";
const char RAFT_LOG_DIR[]  = "log";
#define BRAFT_SNAPSHOT_PATTERN "snapshot_%020" PRId64
#define BRAFT_SNAPSHOT_META_FILE        "__raft_snapshot_meta"
#define BRAFT_SNAPSHOT_ATTACH_META_FILE "__raft_snapshot_attach_meta"
#define BRAFT_PROTOBUF_FILE_TEMP ".tmp"

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTSNAPSHOT_DEFINE_H_
