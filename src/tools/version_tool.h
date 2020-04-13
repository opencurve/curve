/*
 * Project: curve
 * Created Date: 2020-02-18
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_VERSION_TOOL_H_
#define SRC_TOOLS_VERSION_TOOL_H_

#include <string>
#include <map>
#include <vector>
#include <memory>
#include "src/tools/mds_client.h"
#include "src/tools/metric_client.h"
#include "src/common/string_util.h"

namespace curve {
namespace tool {

using VersionMapType = std::map<std::string, std::vector<std::string>>;
using ProcessMapType = std::map<std::string, std::vector<std::string>>;
using ClientVersionMapType = std::map<std::string, VersionMapType>;
const char kOldVersion[] = "before-0.0.5.2";
const char kProcessNebdServer[] = "nebd-server";
const char kProcessQemu[] = "qemu";
const char kProcessPython[] = "python";
const char kProcessOther[] = "other";

class VersionTool {
 public:
    explicit VersionTool(std::shared_ptr<MDSClient> mdsClient,
                         std::shared_ptr<MetricClient> metricClient)
                                 : mdsClient_(mdsClient),
                                   metricClient_(metricClient) {}
    virtual ~VersionTool() {}
 	/**
     *  @brief 初始化channel
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& mdsAddr);

    /**
     *  @brief 获取mds的版本并检查版本一致性
     *  @param[out] version 版本
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAndCheckMdsVersion(std::string* version,
                                      std::vector<std::string>* failedList);

    /**
     *  @brief 获取chunkserver的版本并检查版本一致性
     *  @param[out] version 版本
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAndCheckChunkServerVersion(std::string* version,
                                       std::vector<std::string>* failedList);

    /**
     *  @brief 获取client的版本
     *  @param[out] versionMap process->版本->地址的映射表
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetClientVersion(ClientVersionMapType* versionMap);

    /**
     *  @brief 打印每个version对应的地址
     *  @param versionMap version到地址列表的map
     */
    static void PrintVersionMap(const VersionMapType& versionMap);

    /**
     *  @brief 打印访问失败的地址
     *  @param failedList 访问失败的地址列表
     */
    static void PrintFailedList(const std::vector<std::string>& failedList);

 private:
    /**
     *  @brief 获取addrVec对应地址的version，并把version和地址对应关系存在map中
     *  @param addrVec 地址列表
     *  @param[out] versionMap version到地址的map
     *  @param[out] failedList 查询version失败的地址列表
     */
    void GetVersionMap(const std::vector<std::string>& addrVec,
                       VersionMapType* versionMap,
                       std::vector<std::string>* failedList);

    /**
     *  @brief 获取addrVec对应地址的version，并把version和地址对应关系存在map中
     *  @param addrVec 地址列表
     *  @param[out] processMap 不同的process对应的client的地址列表
     */
    void FetchClientProcessMap(const std::vector<std::string>& addrVec,
                               ProcessMapType* processMap);

    /**
     *  @brief 从启动server的命令行获取对应的程序的名字
     *         比如nebd的命令行为
     *         process_cmdline : "/usr/bin/nebd-server
     *         -confPath=/etc/nebd/nebd-server.conf
     *         -log_dir=/data/log/nebd/server
     *         -graceful_quit_on_sigterm=true
     *         -stderrthreshold=3
     *         "
     *         那么我们要解析出的名字是nebd-server
     *  @param addrVec 地址列表
     *  @return 进程的名字
     */
    std::string GetProcessNameFromCmd(const std::string& cmd);

 private:
    // 向mds发送RPC的client
    std::shared_ptr<MDSClient> mdsClient_;
    // 获取metric的client
    std::shared_ptr<MetricClient> metricClient_;
};

}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_VERSION_TOOL_H_
