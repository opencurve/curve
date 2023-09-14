/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2019-10-10
 * Author: charisu
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
     * @brief printing usage
     * @param command: Query command
     * @return None
     */
    void PrintHelp(const std::string &command) override;

    /**
     * @brief Execute command
     * @param command: The command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string &command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

    /**
     * @brief Compare file and snapshot consistency
     * @return returns 0 for success, -1 for failure
     */
    int Check();

 private:
    /**
     * Initialize
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
