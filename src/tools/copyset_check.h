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
 * Created Date: 2019-10-30
 * Author: charisu
 */

#ifndef SRC_TOOLS_COPYSET_CHECK_H_
#define SRC_TOOLS_COPYSET_CHECK_H_

#include <gflags/gflags.h>

#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <set>
#include <memory>
#include <iterator>

#include "src/mds/common/mds_define.h"
#include "src/tools/copyset_check_core.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

using curve::mds::topology::PoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ServerIdType;

namespace curve {
namespace tool {

class CopysetCheck : public CurveTool {
 public:
    explicit CopysetCheck(std::shared_ptr<CopysetCheckCore> core) :
                        core_(core), inited_(false) {}
    ~CopysetCheck() = default;

    /**
     *  @brief 根据flag检查复制组健康状态
     *  复制组健康的标准，没有任何副本处于以下状态，下面的顺序按优先级排序，
     *  即满足上面一条，就不会检查下面一条
     *  1、leader为空（复制组的信息以leader处的为准，没有leader无法检查）
     *  2、配置中的副本数量不足
     *  3、有副本不在线
     *  4、有副本在安装快照
     *  5、副本间log index差距太大
     *  6、对于集群来说，还要判断一下chunkserver上的copyset数量和leader数量是否均衡，
     *     避免后续会有调度使得集群不稳定
     *  @param command 要执行的命令，目前有check-copyset，check-chunkserver，
     *                 check-server，check-cluster等
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string& command) override;

    /**
     *  @brief 打印帮助信息
     *  @param command 要执行的命令，目前有check-copyset，check-chunkserver，
     *                 check-server，check-cluster等
     */
    void PrintHelp(const std::string& command) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
   /**
     *  @brief 初始化
     */
    int Init();

    /**
     *  @brief 检查单个copyset
     *  @return 健康返回0，其他情况返回-1
     */
    int CheckCopyset();

    /**
     *  @brief 检查chunkserver上所有copyset
     *  @return 健康返回0，其他情况返回-1
     */
    int CheckChunkServer();

    /**
     *  @brief 检查server上所有copyset
     *  @return 健康返回0，其他情况返回-1
     */
    int CheckServer();

    /**
     *  @brief 检查集群所有copyset
     *  @return 健康返回0，其他情况返回-1
     */
    int CheckCopysetsInCluster();

    /**
     *  @brief 检查mds端的operator
     *  @return 无operator返回0，其他情况返回-1
     */
    int CheckOperator(const std::string& opName);

    // 打印copyset检查的详细结果
    void PrintDetail();
    void PrintCopySet(const std::set<std::string>& set);

    // 打印检查的结果，一共多少copyset，有多少不健康
    void PrintStatistic();

    // 打印有问题的chunkserver列表
    void PrintExcepChunkservers();

    // 打印大多数不在线的副本上面的卷
    int PrintMayBrokenVolumes();

 private:
    // 检查copyset的核心逻辑
    std::shared_ptr<CopysetCheckCore> core_;
    // 是否初始化成功过
    bool inited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_CHECK_H_
