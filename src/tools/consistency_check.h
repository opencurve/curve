/*
 * Project: curve
 * File Created: Saturday, 29th June 2019 12:35:00 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_CONSISTENCY_CHECK_H_
#define SRC_TOOLS_CONSISTENCY_CHECK_H_

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <set>
#include <utility>
#include <map>

#include "proto/copyset.pb.h"
#include "src/common/net_common.h"
#include "src/tools/namespace_tool_core.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

DECLARE_string(filename);
DECLARE_bool(check_hash);

namespace curve {
namespace tool {
using CopySet = std::pair<PoolIdType, CopySetIdType>;
using CsAddrsType = std::vector<std::string>;

std::ostream& operator<<(std::ostream& os, const CopySet& copyset);
std::ostream& operator<<(std::ostream& os, const CsAddrsType& csAddrs);

class ConsistencyCheck : public CurveTool {
 public:
    ConsistencyCheck(std::shared_ptr<NameSpaceToolCore> nameSpaceToolCore,
                         std::shared_ptr<ChunkServerClient> csClient);
    ~ConsistencyCheck() = default;

    /**
     *  @brief 打印help信息
     *  @param cmd：执行的命令
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
     *  @brief 检查三副本一致性
     *  @param fileName 要检查一致性的文件名
     *  @param checkHash 是否检查hash，如果为false，检查apply index而不是hash
     *  @return 一致返回0，否则返回-1
     */
    int CheckFileConsistency(const std::string& fileName, bool checkHash);

    /**
     *  @brief 检查copyset的三副本一致性
     *  @param copysetId 要检查的copysetId
     *  @param checkHash 是否检查hash，如果为false，检查apply index而不是hash
     *  @return 成功返回0，失败返回-1
     */
    int CheckCopysetConsistency(const CopySet copysetId,
                                bool checkHash);

    /**
     *  @brief 打印帮助信息
     */
    void PrintHelp();

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
     *  @brief 从mds获取文件所在的copyset列表
     *  @param fileName 文件名
     *  @param[out] copysetIds copysetId的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    int FetchFileCopyset(const std::string& fileName,
                         std::set<CopySet>* copysets);

    /**
     *  @brief 从chunkserver获取copyset的状态
     *  @param csAddr chunkserver地址
     *  @param copysetId 要获取的copysetId
     *  @param[out] response 返回的response
     *  @return 成功返回0，失败返回-1
     */
    int GetCopysetStatusResponse(const std::string& csAddr,
                                 const CopySet copyset,
                                 CopysetStatusResponse* response);

    /**
     *  @brief 检查copyset中指定chunk的hash的一致性
     *  @param copysetId 要检查的copysetId
     *  @param csAddrs copyset对应的chunkserver的地址
     *  @return 一致返回0，否则返回-1
     */
    int CheckCopysetHash(const CopySet& copyset,
                         const CsAddrsType& csAddrs);

    /**
     *  @brief chunk在三个副本的hash的一致性
     *  @param chunk 要检查的chunk
     *  @param csAddrs copyset对应的chunkserver的地址
     *  @return 一致返回0，否则返回-1
     */
    int CheckChunkHash(const Chunk& chunk,
                       const CsAddrsType& csAddrs);

    /**
     *  @brief 检查副本间applyindex的一致性
     *  @param copysetId 要检查的copysetId
     *  @param csAddrs copyset对应的chunkserver的地址
     *  @return 一致返回0，否则返回-1
     */
    int CheckApplyIndex(const CopySet copyset,
                        const CsAddrsType& csAddrs);

 private:
    // 文件所在的逻辑池id
    PoolIdType  lpid_;
    // 用来与mds的nameservice接口交互
    std::shared_ptr<NameSpaceToolCore> nameSpaceToolCore_;
    // 向chunkserver发送RPC的client
    std::shared_ptr<ChunkServerClient> csClient_;
    // copyset中需要检查hash的chunk
    std::map<CopySet, std::set<uint64_t>> chunksInCopyset_;
    // 是否初始化成功过
    bool inited_;
};
}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_CONSISTENCY_CHECK_H_
