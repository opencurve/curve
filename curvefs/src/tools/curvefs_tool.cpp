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
 * Created Date: 2021-11-02
 * Author: chengyi01
 */

#include "curvefs/src/tools/curvefs_tool.h"

namespace curvefs {
namespace tools {

void CurvefsTool::PrintHelp() {
    std::cout << "Example :" << std::endl;
    std::cout << programe_ << " " << command_ << " " << kConfPathHelp;
}

int CurvefsTool::Run() {
    if (Init() < 0) {
        return -1;
    }
    int ret = RunCommand();
    return ret;
}

int CurvefsToolMetric::Init(const std::shared_ptr<MetricClient>& metricClient) {
    metricClient_ = metricClient;
    return 0;
}

void CurvefsToolMetric::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << " [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs << "]"
              << " [-rpcRetryTimes=" << FLAGS_rpcRetryTimes << "]";
}

void CurvefsToolMetric::AddUpdateFlagsFunc(
    const std::function<void(curve::common::Configuration*,
                             google::CommandLineFlagInfo*)>& func) {
    updateFlagsFunc_.push_back(func);
}

int CurvefsToolMetric::RunCommand() {
    int ret = 0;
    for (auto const& i : addr2SubUri) {
        std::string value;
        MetricStatusCode statusCode =
            metricClient_->GetMetric(i.first, i.second, &value);
        AfterGetMetric(i.first, i.second, value, statusCode);
    }

    if (ProcessMetrics() != 0) {
        ret = -1;
    }
    return ret;
}

void CurvefsToolMetric::AddUpdateFlags() {
    // rpcTimeout and rpcRetrytimes is default
    AddUpdateFlagsFunc(SetRpcTimeoutMs);
    AddUpdateFlagsFunc(SetRpcRetryTimes);
}

void CurvefsToolMetric::UpdateFlags() {
    curve::common::Configuration conf;
    conf.SetConfigPath(FLAGS_confPath);
    if (!conf.LoadConfig()) {
        std::cerr << "load configure file " << FLAGS_confPath << " failed!"
                  << std::endl;
    }
    google::CommandLineFlagInfo info;

    for (auto& i : updateFlagsFunc_) {
        i(&conf, &info);
    }
}

void CurvefsToolMetric::AddAddr2Suburi(
    const std::pair<std::string, std::string>& addrSubUri) {
    addr2SubUri.push_back(addrSubUri);
}

int CurvefsToolMetric::Init() {
    // add need update flags
    AddUpdateFlags();
    UpdateFlags();
    InitHostsAddr();
    return 0;
}

}  // namespace tools
}  // namespace curvefs
