/*
 * Project: curve
 * File Created: 20200108
 * Author: lixiaocui
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_SCHEDULE_TOOL_H_
#define SRC_TOOLS_SCHEDULE_TOOL_H_

#include <memory>
#include <string>
#include "src/tools/mds_client.h"
#include "src/tools/curve_tool.h"

namespace curve {
namespace tool {
class ScheduleTool : public CurveTool {
 public:
    explicit ScheduleTool(std::shared_ptr<MDSClient> mdsClient)
        : mdsClient_(mdsClient) {}

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

    /**
     *  @brief 打印help信息
     *  @param cmd：执行的命令
     *  @return 无
     */
    void PrintHelp(const std::string &command) override;

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &command) override;

 private:
    /**
     * @brief PrintRapidLeaderSchedule 打印rapid-leader-schdule的help信息
     */
    void PrintRapidLeaderScheduleHelp();

    /**
     * @brief DoRapidLeaderSchedule 向mds发送rpc进行快速transfer leader
     */
    int DoRapidLeaderSchedule();

 private:
    std::shared_ptr<MDSClient> mdsClient_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SCHEDULE_TOOL_H_
