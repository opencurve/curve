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

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_

#include <gflags/gflags.h>

#include <functional>
#include <string>

#include "src/common/configuration.h"

namespace curvefs {
namespace tools {

// programe name
const char kProgrameName[] = "curvefs-tool";

// version
const char kVersionCmd[] = "version";

// build-topology
const char kBuildTopologyCmd[] = "build-topology";

// umount
const char kUmountCmd[] = "umountfs";

// query-metadata
const char kMetedataUsageCmd[] = "metadata-usage";

// configure
const char kConfPathHelp[] = "[-confPath=/etc/curvefs/tools.conf]";

// kHelp
const char kHelpStr[] =
    "Usage: curve_ops_tool [Command] [OPTIONS...]\n"
    "COMMANDS:\n"  // NOLINT
    "version: show the version of cluster\n"
    "build-topology: build cluster topology based on topo.json\n"
    "umountfs: umount curvefs from local and cluster\n"
    "space-cluster: show the space usage of cluster\n"
    "You can specify the config path by -confPath to avoid typing too many "
    "options\n";  // NOLINT

}  // namespace tools
}  // namespace curvefs

namespace curvefs {
namespace mds {
namespace topology {
// topology
const int kRetCodeCommonErr = -1;
const int kRetCodeRedirectMds = -2;

const char kPools[] = "pools";
const char kPool[] = "pool";
const char kServers[] = "servers";
const char kMetaServer[] = "metaservers";
const char kName[] = "name";
const char kInternalIp[] = "internalip";
const char kInternalPort[] = "internalport";
const char kExternalIp[] = "externalip";
const char kExternalPort[] = "externalport";
const char kZone[] = "zone";
const char kReplicasNum[] = "replicasnum";
const char kCopysetNum[] = "copysetnum";
const char kZoneNum[] = "zonenum";
}  // namespace topology
}  // namespace mds
}  // namespace curvefs

namespace curvefs {
namespace tools {

template <class FlagInfoT>
void SetFlagInfo(curve::common::Configuration* conf,
                 google::CommandLineFlagInfo* info, const std::string& key,
                 FlagInfoT* flag);

extern std::function<void(curve::common::Configuration*,
                          google::CommandLineFlagInfo*)>
    SetMdsAddr;

}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOL_DEFINE_H_
