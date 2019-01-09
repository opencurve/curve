/*
 * Project: curve
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
#define SRC_MDS_NAMESERVER2_CLEAN_CORE_H_

#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/topology/topology.h"
#include "src/mds/nameserver2/define.h"
#include "src/mds/nameserver2/task_progress.h"

using  ::curve::mds::topology::Topology;

namespace curve {
namespace mds {

class CleanCore {
 public:
    explicit CleanCore(NameServerStorage * storage) {
        storage_ = storage;
    }
    /**
     * @brief 删除快照文件，更新task状态
     * @param snapShotFile: 需要清理的snapshot文件
     * @param progress: CleanSnapShotFile接口属于时间较长的偏异步任务
     *                  这里传入进度进行跟踪反馈
     */
    StatusCode CleanSnapShotFile(const FileInfo & snapShotFile,
                                 TaskProgress* progress);

    /**
     * @brief 删除快照文件，更新task状态
     * @param commonFile: 需要清理的snapshot文件
     * @param progress: CleanSnapShotFile接口属于时间较长的偏异步任务
     *                  这里传入进度进行跟踪反馈
     */
    void CleanCommonFile(const FileInfo & commonFile,
                        TaskProgress* progress);

 private:
    bool DeleteSnapShotChunk(LogicalPoolID logicalPoolId,
                        PageFileChunkInfo chunk,
                        SnapShotID snapshotID);

 private:
    NameServerStorage *storage_;
    // TODO(hzsunjianliang): client-for-delete
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
