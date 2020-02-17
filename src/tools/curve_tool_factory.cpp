/*
 * Project: curve
 * Created Date: 2019-12-27
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include "src/tools/curve_tool_factory.h"

namespace curve {
namespace tool {

std::shared_ptr<CurveTool> CurveToolFactory::GenerateCurveTool(
                                    const std::string& command) {
    if (StatusTool::SupportCommand(command)) {
        return GenerateStatusTool();
    } else if (NameSpaceTool::SupportCommand(command)) {
        return GenerateNameSpaceTool();
    } else if (ConsistencyCheck::SupportCommand(command)) {
        return GenerateConsistencyCheck();
    } else if (CurveCli::SupportCommand(command)) {
        return GenerateCurveCli();
    } else if (CopysetCheck::SupportCommand(command)) {
        return GenerateCopysetCheck();
    } else if (ScheduleTool::SupportCommand(command)) {
        return GenerateScheduleTool();
    } else {
        return nullptr;
    }
}

std::shared_ptr<StatusTool> CurveToolFactory::GenerateStatusTool() {
    auto mdsClient = std::make_shared<MDSClient>();
    auto etcdClient = std::make_shared<EtcdClient>();
    auto nameSpaceTool =
        std::make_shared<NameSpaceToolCore>(mdsClient);
    auto csClient = std::make_shared<ChunkServerClient>();
    auto copysetCheck =
        std::make_shared<CopysetCheckCore>(mdsClient, csClient);
    auto metricClient = std::make_shared<MetricClient>();
    auto versionTool = std::make_shared<VersionTool>(mdsClient, metricClient);
    return std::make_shared<StatusTool>(mdsClient, etcdClient,
                                       nameSpaceTool, copysetCheck,
                                       versionTool);
}

std::shared_ptr<NameSpaceTool> CurveToolFactory::GenerateNameSpaceTool() {
    auto client = std::make_shared<MDSClient>();
    auto core = std::make_shared<NameSpaceToolCore>(client);
    return std::make_shared<NameSpaceTool>(core);
}

std::shared_ptr<ConsistencyCheck> CurveToolFactory::GenerateConsistencyCheck() {  //  NOLINT
    auto client = std::make_shared<MDSClient>();
    auto nameSpaceTool =
        std::make_shared<NameSpaceToolCore>(client);
    auto csClient = std::make_shared<ChunkServerClient>();
    return std::make_shared<ConsistencyCheck>(nameSpaceTool, csClient);
}

std::shared_ptr<CurveCli> CurveToolFactory::GenerateCurveCli() {
    return std::make_shared<CurveCli>();
}

std::shared_ptr<CopysetCheck> CurveToolFactory::GenerateCopysetCheck() {
    auto mdsClient = std::make_shared<MDSClient>();
    auto csClient = std::make_shared<ChunkServerClient>();
    auto core = std::make_shared<curve::tool::CopysetCheckCore>(mdsClient,
                                                                csClient);
    return std::make_shared<CopysetCheck>(core);
}

std::shared_ptr<ScheduleTool> CurveToolFactory::GenerateScheduleTool() {
    auto mdsClient = std::make_shared<MDSClient>();
    return std::make_shared<ScheduleTool>(mdsClient);
}

}  // namespace tool
}  // namespace curve
