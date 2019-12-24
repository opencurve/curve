/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CURVE_CLI_H_
#define SRC_TOOLS_CURVE_CLI_H_

#include <gflags/gflags.h>
#include <butil/string_splitter.h>
#include <braft/cli.h>
#include <braft/configuration.h>

#include <map>
#include <string>
#include <iostream>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/cli2.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {
class CurveCli : public CurveTool {
 public:
    /**
     *  @brief 打印help信息
     *  @param 无
     *  @return 无
     */
    void PrintHelp(const std::string &cmd) override;

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &cmd) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     *  @brief 删除peer
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int RemovePeer();

    /**
     *  @brief 转移leader
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int TransferLeader();

    /**
     *  @brief 重置配置组成员，目前只支持reset成一个成员
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int ResetPeer();
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_CLI_H_
