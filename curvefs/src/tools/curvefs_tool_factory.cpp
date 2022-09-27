/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#include "curvefs/src/tools/curvefs_tool_factory.h"

#include "curvefs/src/tools/check/curvefs_copyset_check.h"
#include "curvefs/src/tools/copyset/curvefs_copyset_status.h"
#include "curvefs/src/tools/create/curvefs_create_fs.h"
#include "curvefs/src/tools/create/curvefs_create_topology_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/delete/curvefs_delete_fs_tool.h"
#include "curvefs/src/tools/list/curvefs_copysetinfo_list.h"
#include "curvefs/src/tools/list/curvefs_fsinfo_list.h"
#include "curvefs/src/tools/list/curvefs_topology_list.h"
#include "curvefs/src/tools/query/curvefs_copyset_query.h"
#include "curvefs/src/tools/query/curvefs_fs_query.h"
#include "curvefs/src/tools/query/curvefs_inode_query.h"
#include "curvefs/src/tools/query/curvefs_metaserver_query.h"
#include "curvefs/src/tools/query/curvefs_partition_query.h"
#include "curvefs/src/tools/status/curvefs_copyset_status.h"
#include "curvefs/src/tools/status/curvefs_etcd_status.h"
#include "curvefs/src/tools/status/curvefs_mds_status.h"
#include "curvefs/src/tools/status/curvefs_metaserver_status.h"
#include "curvefs/src/tools/status/curvefs_status.h"
#include "curvefs/src/tools/umount/curvefs_umount_fs_tool.h"
#include "curvefs/src/tools/usage/curvefs_metadata_usage_tool.h"
#include "curvefs/src/tools/version/curvefs_version_tool.h"
#include "curvefs/src/tools/list/curvefs_partition_list.h"
#include "curvefs/src/tools/delete/curvefs_delete_partition_tool.h"
#include "curvefs/src/tools/offline/curvefs_offline_metaserver_tool.h"

namespace curvefs {
namespace tools {

CurvefsToolFactory::CurvefsToolFactory() {
    // version
    RegisterCurvefsTool(std::string(kVersionCmd),
                        CurvefsToolCreator<version::VersionTool>::Create);

    // umount-fs
    RegisterCurvefsTool(std::string(kUmountFsCmd),
                        CurvefsToolCreator<umount::UmountFsTool>::Create);

    // create-topology
    RegisterCurvefsTool(
        std::string(kCreateTopologyCmd),
        CurvefsToolCreator<mds::topology::CurvefsBuildTopologyTool>::Create);

    // create-fs
    RegisterCurvefsTool(std::string(kCreateFsCmd),
                        CurvefsToolCreator<create::CreateFsTool>::Create);

    // usage-metadata
    RegisterCurvefsTool(std::string(kMetedataUsageCmd),
                        CurvefsToolCreator<usage::MatedataUsageTool>::Create);
    // status
    RegisterCurvefsTool(std::string(kStatusCmd),
                        CurvefsToolCreator<status::StatusTool>::Create);
    // status-mds
    RegisterCurvefsTool(std::string(kMdsStatusCmd),
                        CurvefsToolCreator<status::MdsStatusTool>::Create);

    // status-metaserver
    RegisterCurvefsTool(
        std::string(kMetaserverStatusCmd),
        CurvefsToolCreator<status::MetaserverStatusTool>::Create);

    // status-etcd
    RegisterCurvefsTool(std::string(kEtcdStatusCmd),
                        CurvefsToolCreator<status::EtcdStatusTool>::Create);

    // status-copyset
    RegisterCurvefsTool(std::string(kCopysetStatusCmd),
                        CurvefsToolCreator<status::CopysetStatusTool>::Create);

    // list-fs
    RegisterCurvefsTool(std::string(kFsInfoListCmd),
                        CurvefsToolCreator<list::FsInfoListTool>::Create);
    // list-copysetInfo
    RegisterCurvefsTool(std::string(kCopysetInfoListCmd),
                        CurvefsToolCreator<list::CopysetInfoListTool>::Create);

    // list-topology
    RegisterCurvefsTool(std::string(kTopologyListCmd),
                        CurvefsToolCreator<list::TopologyListTool>::Create);

    // list-partition
    RegisterCurvefsTool(std::string(kPartitionListCmd),
                        CurvefsToolCreator<list::PartitionListTool>::Create);

    // query-copyset
    RegisterCurvefsTool(std::string(kCopysetQueryCmd),
                        CurvefsToolCreator<query::CopysetQueryTool>::Create);

    // query-partion
    RegisterCurvefsTool(std::string(kPartitionQueryCmd),
                        CurvefsToolCreator<query::PartitionQueryTool>::Create);

    // query-metaserver
    RegisterCurvefsTool(std::string(kMetaserverQueryCmd),
                        CurvefsToolCreator<query::MetaserverQueryTool>::Create);

    // query-fs
    RegisterCurvefsTool(std::string(kFsQueryCmd),
                        CurvefsToolCreator<query::FsQueryTool>::Create);

    // query-inode
    RegisterCurvefsTool(std::string(kInodeQueryCmd),
                        CurvefsToolCreator<query::InodeQueryTool>::Create);

    // delete-fs
    RegisterCurvefsTool(std::string(kDeleteFsCmd),
                        CurvefsToolCreator<delete_::DeleteFsTool>::Create);

    // delete-partition
    RegisterCurvefsTool(std::string(kPartitionDeleteCmd),
                    CurvefsToolCreator<delete_::DeletePartitionTool>::Create);

    // check-copyset
    RegisterCurvefsTool(std::string(kCopysetCheckCmd),
                        CurvefsToolCreator<check::CopysetCheckTool>::Create);

    // offline-metaserver
    RegisterCurvefsTool(std::string(kMetaserverOfflineCmd),
        CurvefsToolCreator<offline::OfflineMetaserverTool>::Create);
}

std::shared_ptr<CurvefsTool> CurvefsToolFactory::GenerateCurvefsTool(
    const std::string& command) {
    auto search = command2creator_.find(command);
    if (search != command2creator_.end()) {
        return search->second();
    }
    return nullptr;
}

void CurvefsToolFactory::RegisterCurvefsTool(
    const std::string& command,
    const std::function<std::shared_ptr<CurvefsTool>()>& function) {
    command2creator_.insert({command, function});
}

}  // namespace tools
}  // namespace curvefs
