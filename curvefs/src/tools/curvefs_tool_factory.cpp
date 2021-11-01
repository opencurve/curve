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

namespace curvefs {
namespace tools {

CurvefsToolFactory::CurvefsToolFactory() {
    // version
    RegisterCurvefsTool(std::string(kVersionCmd),
                        CurvefsToolCreator<version::VersionTool>::Create);

    // umountfs
    RegisterCurvefsTool(std::string(kUmountCmd),
                        CurvefsToolCreator<umountfs::UmountfsTool>::Create);

    // build-topology
    RegisterCurvefsTool(
        std::string(kBuildTopologyCmd),
        CurvefsToolCreator<mds::topology::CurvefsBuildTopologyTool>::Create);

    // metadata-usage
    RegisterCurvefsTool(std::string(kMetedataUsageCmd),
                        CurvefsToolCreator<space::MatedataUsageTool>::Create);

    // mds-status
    RegisterCurvefsTool(std::string(kMdsStatusCmd),
                        CurvefsToolCreator<status::MdsStatusTool>::Create);
    // metaserver-status
    RegisterCurvefsTool(
        std::string(kMetaserverStatusCmd),
        CurvefsToolCreator<status::MetaserverStatusTool>::Create);

    // etcd-status
    RegisterCurvefsTool(std::string(kEtcdStatusCmd),
                        CurvefsToolCreator<status::EtcdStatusTool>::Create);

    // copysets-status
    RegisterCurvefsTool(
        std::string(kCopysetsStatusCmd),
        CurvefsToolCreator<status::ClusterCopysetStatusTool>::Create);

    // copyset-query
    RegisterCurvefsTool(std::string(kCopysetQueryCmd),
                        CurvefsToolCreator<query::CopysetQueryTool>::Create);

    // partition-list
    RegisterCurvefsTool(std::string(kPartitionListCmd),
                        CurvefsToolCreator<query::PartitionListTool>::Create);

    // fsinfo-list
    RegisterCurvefsTool(std::string(kFsInfoListCmd),
                        CurvefsToolCreator<query::FsInfoListTool>::Create);
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
