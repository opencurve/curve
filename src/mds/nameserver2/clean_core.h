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
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
#define SRC_MDS_NAMESERVER2_CLEAN_CORE_H_

#include <memory>
#include <string>
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/task_progress.h"
#include "src/mds/chunkserverclient/copyset_client.h"
#include "src/mds/topology/topology.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

using ::curve::mds::chunkserverclient::CopysetClient;
using ::curve::mds::topology::Topology;

namespace curve {
namespace mds {

class CleanCore {
 public:
    CleanCore(std::shared_ptr<NameServerStorage> storage,
        std::shared_ptr<CopysetClient> copysetClient,
        std::shared_ptr<AllocStatistic> allocStatistic)
        : storage_(storage),
          copysetClient_(copysetClient),
          allocStatistic_(allocStatistic) {}

    /**
     * @brief Delete the snapshot file and update the task status
     * @param snapshotFile: The snapshot file that needs to be cleaned
     * @param progress: The CleanSnapShotFile interface is a relatively asynchronous task that takes a long time
     *                  Here, progress is transmitted for tracking and feedback
     */
    StatusCode CleanSnapShotFile(const FileInfo & snapShotFile,
                                 TaskProgress* progress);

    /**
     * @brief Delete regular files and update task status
     * @param commonFile: A regular file that needs to be cleaned
     * @param progress: The CleanFile interface is a relatively asynchronous task that takes a long time
     *                  Here, progress is transmitted for tracking and feedback
     * @return whether the execution was successful, and if successful, return StatusCode::kOK
     */
    StatusCode CleanFile(const FileInfo & commonFile,
                        TaskProgress* progress);

    /**
     * @brief clean discarded segment and chunks
     */
    StatusCode CleanDiscardSegment(const std::string& cleanSegmentKey,
                                   const DiscardSegmentInfo& discardSegmentInfo,
                                   TaskProgress* progress);

 private:
    int DeleteChunksInSegment(const PageFileSegment& segment,
                              const SeqNum& seq);

    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<CopysetClient> copysetClient_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
