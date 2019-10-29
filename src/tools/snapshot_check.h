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

namespace curve {
namespace tool {
class SnapshotCheck {
 public:
    SnapshotCheck(std::shared_ptr<curve::client::FileClient> client,
                  std::shared_ptr<SnapshotRead> snapshot) :
                        client_(client), snapshot_(snapshot) {}
    ~SnapshotCheck() = default;

    /**
     * 初始化
     */
    int Init();

    /**
     * 释放资源
     */
    void UnInit();

    /**
     * 打印帮助信息
     */
    void PrintHelp();

    /**
     *  @brief 比较文件和快照的一致性
     *  @return 成功返回0，失败返回-1
     */
    int Check();

 private:
    std::shared_ptr<curve::client::FileClient> client_;
    std::shared_ptr<SnapshotRead> snapshot_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SNAPSHOT_CHECK_H_
