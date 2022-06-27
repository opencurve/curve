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
 * Created Date: 2019-11-25
 * Author: charisu
 */

#ifndef SRC_TOOLS_MDS_CLIENT_H_
#define SRC_TOOLS_MDS_CLIENT_H_

#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <json/json.h>

#include <vector>
#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <unordered_map>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/schedule.pb.h"
#include "src/common/authenticator.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/common/net_common.h"
#include "src/tools/metric_name.h"
#include "src/tools/metric_client.h"
#include "src/tools/common.h"
#include "src/tools/curve_tool_define.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::mds::PageFileChunkInfo;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::mds::topology::ChunkServerInfo;
using curve::common::ChunkServerLocation;
using curve::mds::topology::CopySetServerInfo;
using curve::mds::topology::ServerInfo;
using curve::mds::topology::ZoneInfo;
using curve::mds::topology::PhysicalPoolInfo;
using curve::mds::topology::LogicalPoolInfo;
using curve::common::CopysetInfo;
using curve::mds::topology::ServerIdType;
using curve::mds::topology::ZoneIdType;
using curve::mds::topology::PoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::ListChunkServerRequest;
using curve::mds::topology::GetChunkServerInfoRequest;
using curve::mds::topology::GetCopySetsInChunkServerRequest;
using curve::mds::schedule::RapidLeaderScheduleRequst;
using curve::mds::schedule::RapidLeaderScheduleResponse;
using curve::common::Authenticator;

namespace curve {
namespace tool {

enum class GetSegmentRes {
    kOK = 0,   // 获取segment成功
    kSegmentNotAllocated = -1,  // segment不存在
    kFileNotExists = -2,  // 文件不存在
    kOtherError = -3  // 其他错误
};

using AllocMap = std::unordered_map<PoolIdType, uint64_t>;

class MDSClient {
 public:
    MDSClient() : currentMdsIndex_(0), userName_(""),
                  password_(""), isInited_(false) {}
    virtual ~MDSClient() = default;

    /**
     *  @brief 初始化channel
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& mdsAddr);

    /**
     *  @brief 初始化channel
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @param dummyPort dummy port列表，只输入一个的话
     *         所有mds用同样的dummy port，用字符串分隔有多个的话
     *         为每个mds设置不同的dummy port
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& mdsAddr,
                     const std::string& dummyPort);

    /**
     *  @brief 获取文件fileInfo
     *  @param fileName 文件名
     *  @param[out] fileInfo 文件fileInfo，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetFileInfo(const std::string& fileName, FileInfo* fileInfo);

    /**
     *  @brief 获取文件或目录分配大小
     *  @param fileName 文件名
     *  @param[out] allocSize 文件或目录分配大小，返回值为0时有效
     *  @param[out] allocMap 文件在各个池子分配的情况
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAllocatedSize(const std::string& fileName,
                                 uint64_t* allocSize,
                                 AllocMap* allocMap = nullptr);

    /**
     *  @brief 获取文件或目录的大小
     *  @param fileName 文件名
     *  @param[out] fileSize 文件或目录分配大小，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetFileSize(const std::string& fileName,
                            uint64_t* fileSize);

    /**
     *  @brief 将目录下所有的fileInfo列出来
     *  @param dirName 目录名
     *  @param[out] files 目录下的所有文件fileInfo，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListDir(const std::string& dirName,
                        std::vector<FileInfo>* files);

    /**
     *  @brief 获取指定偏移的segment放到segment里面
     *  @param fileName 文件名
     *  @param offset 偏移值
     *  @param[out] segment 文件中指定偏移的segmentInfo，返回值为0时有效
     *  @return 返回GetSegmentRes，区分segment未分配和其他错误
     */
    virtual GetSegmentRes GetSegmentInfo(const std::string& fileName,
                                 uint64_t offset,
                                 PageFileSegment* segment);

    /**
     *  @brief 删除文件
     *  @param fileName 文件名
     *  @param forcedelete 是否强制删除
     *  @return 成功返回0，失败返回-1
     */
    virtual int DeleteFile(const std::string& fileName,
                           bool forcedelete = false);

    /**
     *  @brief 创建pageFile文件
     *  @param fileName 文件名
     *  @param length 文件长度
     *  @return 成功返回0，失败返回-1
     */
    virtual int CreateFile(const std::string& fileName, uint64_t length);

    /**
     *  @brief List all volumes on copysets
     *  @param copysets
     *  @param[out] fileNames volumes name
     *  @return return 0 when success, -1 when fail
     */
    virtual int ListVolumesOnCopyset(
                        const std::vector<common::CopysetInfo>& copysets,
                        std::vector<std::string>* fileNames);

    /**
     *  @brief 列出client的dummyserver的地址
     *  @param[out] clientAddrs client地址列表，返回0时有效
     *  @param[out] listClientsInRepo 把数据库里的client也列出来
     *  @return 成功返回0,失败返回-1
     */
    virtual int ListClient(std::vector<std::string>* clientAddrs,
                           bool listClientsInRepo = false);

    /**
     *  @brief 获取copyset中的chunkserver列表
     *  @param logicalPoolId 逻辑池id
     *  @param copysetId copyset id
     *  @param[out] csLocs chunkserver位置的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetChunkServerListInCopySet(const PoolIdType& logicalPoolId,
                                     const CopySetIdType& copysetId,
                                     std::vector<ChunkServerLocation>* csLocs);

    /**
     *  @brief 获取copyset中的chunkserver列表
     *  @param logicalPoolId 逻辑池id
     *  @param copysetIds 要查询的copysetId的列表
     *  @param[out] csServerInfos copyset成员的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetChunkServerListInCopySets(const PoolIdType& logicalPoolId,
                            const std::vector<CopySetIdType>& copysetIds,
                            std::vector<CopySetServerInfo>* csServerInfos);

    /**
     *  @brief 获取集群中的物理池列表
     *  @param[out] pools 物理池信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListPhysicalPoolsInCluster(
                        std::vector<PhysicalPoolInfo>* pools);


    /**
     *  @brief 获取物理池中的逻辑池列表
     *  @param id 物理池id
     *  @param[out] pools 逻辑池信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListLogicalPoolsInPhysicalPool(const PoolIdType& id,
                                       std::vector<LogicalPoolInfo>* pools);

    /**
     *  @brief 集群中的逻辑池列表
     *  @param[out] pools 逻辑池信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListLogicalPoolsInCluster(std::vector<LogicalPoolInfo>* pools);

    /**
     *  @brief 获取物理池中的zone列表
     *  @param id 物理池id
     *  @param[out] zones zone信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListZoneInPhysicalPool(const PoolIdType& id,
                               std::vector<ZoneInfo>* zones);

    /**
     *  @brief 获取zone中的server列表
     *  @param id zone id
     *  @param[out] servers server信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListServersInZone(const ZoneIdType& id,
                          std::vector<ServerInfo>* servers);

    /**
     *  @brief 获取server上的chunkserver的列表
     *  @param id server id
     *  @param[out] chunkservers chunkserver信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListChunkServersOnServer(const ServerIdType& id,
                                 std::vector<ChunkServerInfo>* chunkservers);

    /**
     *  @brief 获取server上的chunkserver的列表
     *  @param ip server ip
     *  @param[out] chunkservers chunkserver信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListChunkServersOnServer(const std::string& ip,
                                 std::vector<ChunkServerInfo>* chunkservers);

    /**
     *  @brief 获取chunkserver的详细信息
     *  @param id chunkserver id
     *  @param[out] chunkserver chunkserver的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetChunkServerInfo(const ChunkServerIdType& id,
                           ChunkServerInfo* chunkserver);

    /**
     *  @brief 获取chunkserver的详细信息
     *  @param csAddr chunkserver的地址，ip:port的格式
     *  @param[out] chunkserver chunkserver的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetChunkServerInfo(const std::string& csAddr,
                           ChunkServerInfo* chunkserver);

    /**
     *  @brief 获取chunkserver上的所有copyset
     *  @param id chunkserver的id
     *  @param[out] copysets chunkserver上copyset的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetCopySetsInChunkServer(const ChunkServerIdType& id,
                                 std::vector<CopysetInfo>* copysets);

    /**
     *  @brief 获取chunkserver上的所有copyset
     *  @param csAddr chunkserver的地址，ip:port的格式
     *  @param[out] copysets chunkserver上copyset的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetCopySetsInChunkServer(const std::string& csAddr,
                                 std::vector<CopysetInfo>* copysets);

    /**
     *  @brief 获取集群中的所有copyset
     *  @param[out] copysets 集群中copyset的列表
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetCopySetsInCluster(std::vector<CopysetInfo>* copysets);

    /**
     *  @brief 列出集群中的所有server
     *  @param[out] servers server信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListServersInCluster(std::vector<ServerInfo>* servers);

    /**
     *  @brief 列出集群中的所有chunkserver
     *  @param[out] chunkservers server信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int ListChunkServersInCluster(
                        std::vector<ChunkServerInfo>* chunkservers);

    /**
     *  @brief list all the chunkservers with poolid in cluster
     *  @param[out] chunkservers chunkserver info
     *  @return succeed return 0; failed return -1;
     */
    virtual int ListChunkServersInCluster(std::map<PoolIdType,
                            std::vector<ChunkServerInfo>>* chunkservers);

    /**
     *  @brief 获取mds的某个metric的值
     *  @param metricName metric的名字
     *  @param[out] value metric的值，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetMetric(const std::string& metricName, uint64_t* value);

    /**
     *  @brief 获取mds的某个metric的值
     *  @param metricName metric的名子
     *  @param[out] value metric的值，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetMetric(const std::string& metricName, std::string* value);

    /**
     *  @brief 设置userName，访问namespace接口的时候调用
     *  @param userName 用户名
     */
    void SetUserName(const std::string& userName) {
        userName_ = userName;
    }

    /**
     *  @brief 设置password，访问namespace接口的时候调用
     *  @param password 密码
     */
    void SetPassword(const std::string& password) {
        password_ = password;
    }

    /**
     *  @brief 获取mds地址列表
     *  @return mds地址的列表
     */
    virtual const std::vector<std::string>& GetMdsAddrVec() const {
        return mdsAddrVec_;
    }

    virtual const std::map<std::string, std::string>& GetDummyServerMap()
                                                        const {
        return dummyServerMap_;
    }

    /**
     *  @brief 获取当前mds的地址
     */
    virtual std::vector<std::string> GetCurrentMds();

    /**
     * @brief 向mds发送rpc触发快速leader均衡
     */
    virtual int RapidLeaderSchedule(PoolIdType lpid);

    /**
     *  @brief 获取mds在线状态,
     *          dummyserver在线且dummyserver记录的listen addr
     *          与mds地址一致才认为在线
     *  @param[out] onlineStatus mds在线状态，返回0时有效
     *  @return 成功返回0,失败返回-1
     */
    virtual void GetMdsOnlineStatus(std::map<std::string, bool>* onlineStatus);

    /**
     *  @brief 获取指定chunkserver的恢复状态
     *  @param[in] cs 需要查询的chunkserver列表
     *  @param[out] statusMap 返回各chunkserver对应的恢复状态
     *  @return 成功返回0，失败返回-1
     */
    int QueryChunkServerRecoverStatus(
        const std::vector<ChunkServerIdType>& cs,
        std::map<ChunkServerIdType, bool> *statusMap);

 private:
    /**
     *  @brief 切换mds
     *  @return 切换成功返回true，所有mds都失败则返回false
     */
    bool ChangeMDServer();

    /**
     *  @brief 向mds发送RPC，为了复用代码
     *  @param
     *  @return 成功返回0，失败返回-1
     */
    template <typename T, typename Request, typename Response>
    int SendRpcToMds(Request* request, Response* response, T* obp,
                void (T::*func)(google::protobuf::RpcController*,
                            const Request*, Response*,
                            google::protobuf::Closure*));

    /**
     *  @brief 获取server上的chunkserver的列表
     *  @param request 要发送的request
     *  @param[out] chunkservers chunkserver信息的列表，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    int ListChunkServersOnServer(ListChunkServerRequest* request,
                                 std::vector<ChunkServerInfo>* chunkservers);

    /**
     *  @brief 获取chunkserver的详细信息
     *  @param request 要发送的request
     *  @param[out] chunkserver chunkserver的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    int GetChunkServerInfo(GetChunkServerInfoRequest* request,
                                  ChunkServerInfo* chunkserver);

    /**
     *  @brief 获取chunkserver的详细信息
     *  @param request 要发送的request
     *  @param[out] copysets chunkserver上copyset的详细信息，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    int GetCopySetsInChunkServer(
                            GetCopySetsInChunkServerRequest* request,
                            std::vector<CopysetInfo>* copysets);

    /**
     *  @brief 初始化dummy server地址
     *  @param dummyPort dummy server端口列表
     *  @return 成功返回0，失败返回-1
     */
    int InitDummyServerMap(const std::string& dummyPort);

    /**
     *  @brief 通过dummyServer获取mds的监听地址
     *  @param dummyAddr dummyServer的地址
     *  @param[out] listenAddr mds的监听地址
     *  @return 成功返回0，失败返回-1
     */
    int GetListenAddrFromDummyPort(const std::string& dummyAddr,
                                   std::string* listenAddr);


    // 填充signature
    template <class T>
    void FillUserInfo(T* request);

    // 用于发送http请求的client
    MetricClient metricClient_;
    // 向mds发送RPC的channel
    brpc::Channel channel_;
    // 保存mds地址的vector
    std::vector<std::string> mdsAddrVec_;
    // 保存mds地址对应的dummy server的地址
    std::map<std::string, std::string> dummyServerMap_;
    // 保存当前mds在mdsAddrVec_中的索引
    int currentMdsIndex_;
    // 用户名
    std::string userName_;
    // 密码
    std::string password_;
    // 避免重复初始化
    bool isInited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_MDS_CLIENT_H_
