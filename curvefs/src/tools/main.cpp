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
 * Created Date: 2021-09-03
 * Author: wanghai01
 */

#include "curvefs/src/tools/curvefs_topology_tool.h"

DEFINE_string(op, "", "operation: build_topology");

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = 0;
    curvefs::mds::topology::CurvefsTopologyTool tools;
    if (tools.Init() < 0) {
        LOG(ERROR) << "CurvefsTopologyTool init error.";
        return kRetCodeCommonErr;
    }

    ret = tools.InitTopoData();
    if (ret < 0) {
        LOG(ERROR) << "Init topo json data error.";
        return ret;
    }

    int maxTry = tools.GetMaxTry();
    int retry = 0;
    for (; retry < maxTry; retry++) {
        ret = tools.TryAnotherMdsAddress();
        if (ret < 0) {
            return kRetCodeCommonErr;
        }

        std::string operation = FLAGS_op;
        if (operation == "build_topology") {
            ret = tools.HandleBuildCluster();
        } else {
            LOG(ERROR) << "undefined op.";
            ret = kRetCodeCommonErr;
            break;
        }
        if (ret != kRetCodeRedirectMds) {
            break;
        }
    }

    if (retry >= maxTry) {
        LOG(ERROR) << "rpc retry times exceed.";
        return kRetCodeCommonErr;
    }
    if (ret != 0) {
        LOG(ERROR) << "exec fail, ret = " << ret;
    } else {
        LOG(INFO) << "exec success, ret = " << ret;
    }

    return ret;
}
