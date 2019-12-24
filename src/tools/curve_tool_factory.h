/*
 * Project: curve
 * Created Date: 2019-12-27
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CURVE_TOOL_FACTORY_H_
#define SRC_TOOLS_CURVE_TOOL_FACTORY_H_

#include <string>
#include <memory>
#include <set>

#include "src/tools/curve_tool.h"
#include "src/tools/status_tool.h"
#include "src/tools/namespace_tool.h"
#include "src/tools/consistency_check.h"
#include "src/tools/curve_cli.h"
#include "src/tools/copyset_check.h"

namespace curve {
namespace tool {

class CurveToolFactory {
 public:
    /**
     *  @brief 根据输入的command获取CurveTool对象
     *  @param command 要执行的命令的名称
     *  @return CurveTool实例
     */
    static std::shared_ptr<CurveTool> GenerateCurveTool(
                                    const std::string& command);

 private:
    /**
     *  @brief 获取StatusTool实例
     */
    static std::shared_ptr<StatusTool> GenerateStatusTool();

    /**
     *  @brief 获取NameSpaceTool实例
     */
    static std::shared_ptr<NameSpaceTool> GenerateNameSpaceTool();

    /**
     *  @brief 获取ConsistencyCheck实例
     */
    static std::shared_ptr<ConsistencyCheck> GenerateConsistencyCheck();

    /**
     *  @brief 获取CurveCli实例
     */
    static std::shared_ptr<CurveCli> GenerateCurveCli();

    /**
     *  @brief 获取CopysetCheck实例
     */
    static std::shared_ptr<CopysetCheck> GenerateCopysetCheck();
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_FACTORY_H_
