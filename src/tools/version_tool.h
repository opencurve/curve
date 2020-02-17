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

namespace curve {
namespace tool {

using VersionMapType = std::map<std::string, std::vector<std::string>>;

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
    virtual int GetAndCheckMdsVersion(std::string* version);

    /**
     *  @brief 获取chunkserver的版本并检查版本一致性
     *  @param[out] version 版本
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAndCheckChunkServerVersion(std::string* version);

    /**
     *  @brief 获取client的版本
     *  @param[out] versionMap 版本到地址的map
     *  @param[out] failedList 查询version失败的地址列表
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetClientVersion(VersionMapType* versionMap,
                                 std::vector<std::string>* failedList);

    /**
     *  @brief 打印每个version对应的地址
     *  @param versionMap version到地址列表的map
     */
    void PrintVersionMap(const VersionMapType& versionMap);

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
     *  @brief 打印访问失败的地址
     *  @param failedList 访问失败的地址列表
     */
    void PrintFailedList(const std::vector<std::string>& failedList);

 private:
    // 向mds发送RPC的client
    std::shared_ptr<MDSClient> mdsClient_;
    // 获取metric的client
    std::shared_ptr<MetricClient> metricClient_;
};

}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_VERSION_TOOL_H_
