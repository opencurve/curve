/*
 * Project: curve
 * Created Date: 2019-12-27
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CURVE_TOOL_H_
#define SRC_TOOLS_CURVE_TOOL_H_

#include <string>

namespace curve {
namespace tool {

class CurveTool {
 public:
    virtual int RunCommand(const std::string& command) = 0;
    virtual void PrintHelp(const std::string& command) = 0;
    virtual ~CurveTool() {}
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_H_
