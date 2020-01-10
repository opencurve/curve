/*
 * Project: curve
 * Created Date: 2019-10-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_SNAPSHOT_CHECK_H_
#define SRC_TOOLS_SNAPSHOT_CHECK_H_

#include <gflags/gflags.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>
#include <iostream>

#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"
#include "src/common/crc32.h"
#include "src/tools/snapshot_read.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {
class SnapshotCheck : public CurveTool {
 public:
    SnapshotCheck(std::shared_ptr<curve::client::FileClient> client,
                  std::shared_ptr<SnapshotRead> snapshot) :
                        client_(client), snapshot_(snapshot), inited_(false) {}
    ~SnapshotCheck();


    /**
     *  @brief 打印用法
     *  @param command：查询的命令
     *  @return 无
     */
    void PrintHelp(const std::string &command) override;

    /**
     *  @brief 执行命令
     *  @param command：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &command) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

    /**
     *  @brief 比较文件和快照的一致性
     *  @return 成功返回0，失败返回-1
     */
    int Check();

 private:
    /**
     * 初始化
     */
    int Init();

 private:
    std::shared_ptr<curve::client::FileClient> client_;
    std::shared_ptr<SnapshotRead> snapshot_;
    bool inited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SNAPSHOT_CHECK_H_
