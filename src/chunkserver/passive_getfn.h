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
 * Created Date: Tuesday June 18th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_PASSIVE_GETFN_H_
#define SRC_CHUNKSERVER_PASSIVE_GETFN_H_

#include "src/chunkserver/trash.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/chunkserver/raftlog/curve_segment_log_storage.h"

namespace curve {
namespace chunkserver {

    /**
     * Obtain the number of chunk files in the datastore
     * @param arg: Object pointer to datastore
     */
    uint32_t GetDatastoreChunkCountFunc(void* arg);
    /**
     * @brief: Get the number of WAL segment in CurveSegmentLogStorage
     * @param arg: The pointer to CurveSegmentLogStorage
     */
    uint32_t GetLogStorageWalSegmentCountFunc(void* arg);
    /**
     * Obtain the number of snapshot chunks in the datastore
     * @param arg: Object pointer to datastore
     */
    uint32_t GetDatastoreSnapshotCountFunc(void* arg);
    /**
     * Obtain the number of clone chunks in the datastore
     * @param arg: Object pointer to datastore
     */
    uint32_t GetDatastoreCloneChunkCountFunc(void* arg);
    /**
     * Obtain the number of chunk files on the chunkserver
     * @param arg: nullptr
     */
    uint32_t GetTotalChunkCountFunc(void* arg);
    /**
     * @brief: Get the total number of WAL segment in chunkserver
     * @param arg: The pointer to ChunkServerMetric
     */
    uint32_t GetTotalWalSegmentCountFunc(void* arg);

    /**
     * Obtain the number of snapshot chunks on the chunkserver
     * @param arg: nullptr
     */
    uint32_t GetTotalSnapshotCountFunc(void* arg);
    /**
     * Obtain the number of clone chunks on the chunkserver
     * @param arg: nullptr
     */
    uint32_t GetTotalCloneChunkCountFunc(void* arg);
    /**
     * Obtain the number of remaining chunks in the chunkfilepool
     * @param arg: Object pointer to chunkfilepool
     */
    uint32_t GetChunkLeftFunc(void* arg);
    /**
     * Obtain the number of remaining chunks in the walfilepool
     * @param arg: Object pointer to walfilepool
     */
    uint32_t GetWalSegmentLeftFunc(void* arg);
    /**
     * Obtain the number of chunks in the trash
     * @param arg: Object pointer to trash
     */
    uint32_t GetChunkTrashedFunc(void* arg);

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_PASSIVE_GETFN_H_
