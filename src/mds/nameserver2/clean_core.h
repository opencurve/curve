/*
 * Project: curve
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
#define SRC_MDS_NAMESERVER2_CLEAN_CORE_H_

#include <memory>
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
     * @brief 删除快照文件，更新task状态
     * @param snapShotFile: 需要清理的snapshot文件
     * @param progress: CleanSnapShotFile接口属于时间较长的偏异步任务
     *                  这里传入进度进行跟踪反馈
     */
    StatusCode CleanSnapShotFile(const FileInfo & snapShotFile,
                                 TaskProgress* progress);

    /**
     * @brief 删除普通文件，更新task状态
     * @param commonFile: 需要清理的普通文件
     * @param progress: CleanFile接口属于时间较长的偏异步任务
     *                  这里传入进度进行跟踪反馈
     * @return 是否执行成功，成功返回StatusCode::kOK
     */
    StatusCode CleanFile(const FileInfo & commonFile,
                        TaskProgress* progress);

 private:
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<CopysetClient> copysetClient_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_CLEAN_CORE_H_
