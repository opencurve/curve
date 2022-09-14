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
 * File Created: Monday, 18th February 2019 6:25:17 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_MDS_CLIENT_H_
#define SRC_CLIENT_MDS_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <map>
#include <string>
#include <vector>
#include <list>

#include "include/client/libcurve.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/client/mds_client_base.h"
#include "src/client/metacache_struct.h"

namespace curve {
namespace client {

class RPCExcutorRetryPolicy {
 public:
    RPCExcutorRetryPolicy()
        : retryOpt_(), currentWorkingMDSAddrIndex_(0), cntlID_(1) {}

    void SetOption(const MetaServerOption::RpcRetryOption &option) {
        retryOpt_ = option;
    }
    using RPCFunc = std::function<int(int addrindex, uint64_t rpctimeoutMS,
                                      brpc::Channel *, brpc::Controller *)>;
    /**
     * 将client与mds的重试相关逻辑抽离
     * @param: task为当前要进行的具体rpc任务
     * @param: maxRetryTimeMS是当前执行最大的重试时间
     * @return: 返回当前RPC的结果
     */
    int DoRPCTask(RPCFunc task, uint64_t maxRetryTimeMS);

    /**
     * 测试使用: 设置当前正在服务的mdsindex
     */
    void SetCurrentWorkIndex(int index) {
        currentWorkingMDSAddrIndex_.store(index);
    }

    /**
     * 测试使用：获取当前正在服务的mdsindex
     */
    int GetCurrentWorkIndex() const {
        return currentWorkingMDSAddrIndex_.load();
    }

 private:
    /**
     * rpc失败需要重试，根据cntl返回的不同的状态，确定应该做什么样的预处理。
     * 主要做了以下几件事：
     * 1. 如果上一次的RPC是超时返回，那么执行rpc 超时指数退避逻辑
     * 2. 如果上一次rpc返回not connect等返回值，会主动触发切换mds地址重试
     * 3. 更新重试信息，比如在当前mds上连续重试的次数
     * @param[in]: status为当前rpc的失败返回的状态
     * @param normalRetryCount The total count of normal retry
     * @param[in][out]: curMDSRetryCount当前mds节点上的重试次数，如果切换mds
     *             该值会被重置为1.
     * @param[in]: curRetryMDSIndex代表当前正在重试的mds索引
     * @param[out]: lastWorkingMDSIndex上一次正在提供服务的mds索引
     * @param[out]: timeOutMS根据status对rpctimeout进行调整
     *
     * @return: 返回下一次重试的mds索引
     */
    int PreProcessBeforeRetry(int status, bool retryUnlimit,
                              uint64_t *normalRetryCount,
                              uint64_t *curMDSRetryCount, int curRetryMDSIndex,
                              int *lastWorkingMDSIndex, uint64_t *timeOutMS);
    /**
     * 执行rpc发送任务
     * @param[in]: mdsindex为mds对应的地址索引
     * @param[in]: rpcTimeOutMS是rpc超时时间
     * @param[in]: task为待执行的任务
     * @return: channel获取成功则返回0，否则-1
     */
    int ExcuteTask(int mdsindex, uint64_t rpcTimeOutMS,
                   RPCExcutorRetryPolicy::RPCFunc task);
    /**
     * 根据输入状态获取下一次需要重试的mds索引，mds切换逻辑：
     * 记录三个状态：curRetryMDSIndex、lastWorkingMDSIndex、
     *             currentWorkingMDSIndex
     * 1. 开始的时候curRetryMDSIndex = currentWorkingMDSIndex
     *            lastWorkingMDSIndex = currentWorkingMDSIndex
     * 2.
     * 如果rpc失败，会触发切换curRetryMDSIndex，如果这时候lastWorkingMDSIndex
     *    与currentWorkingMDSIndex相等，这时候会顺序切换到下一个mds索引，
     *    如果lastWorkingMDSIndex与currentWorkingMDSIndex不相等，那么
     *    说明有其他接口更新了currentWorkingMDSAddrIndex_，那么本次切换
     *    直接切换到currentWorkingMDSAddrIndex_
     * @param[in]: needChangeMDS表示当前外围需不需要切换mds，这个值由
     *              PreProcessBeforeRetry函数确定
     * @param[in]: currentRetryIndex为当前正在重试的mds索引
     * @param[in][out]:
     * lastWorkingindex为上一次正在服务的mds索引，正在重试的mds
     *              与正在服务的mds索引可能是不同的mds。
     * @return: 返回下一次要重试的mds索引
     */
    int GetNextMDSIndex(bool needChangeMDS, int currentRetryIndex,
                        int *lastWorkingindex);
    /**
     * 根据输入参数，决定是否继续重试，重试退出条件是重试时间超出最大允许时间
     * IO路径上和非IO路径上的重试时间不一样，非IO路径的重试时间由配置文件的
     * mdsMaxRetryMS参数指定，IO路径为无限循环重试。
     * @param[in]: startTimeMS
     * @param[in]: maxRetryTimeMS为最大重试时间
     * @return:需要继续重试返回true， 否则返回false
     */
    bool GoOnRetry(uint64_t startTimeMS, uint64_t maxRetryTimeMS);

    /**
     * 递增controller id并返回id
     */
    uint64_t GetLogId() {
        return cntlID_.fetch_add(1, std::memory_order_relaxed);
    }

 private:
    // 执行rpc时必要的配置信息
    MetaServerOption::RpcRetryOption retryOpt_;

    // 记录上一次重试过的leader信息
    std::atomic<int> currentWorkingMDSAddrIndex_;

    // controller id，用于trace整个rpc IO链路
    // 这里直接用uint64即可，在可预测的范围内，不会溢出
    std::atomic<uint64_t> cntlID_;
};


struct LeaseRefreshResult;

// MDSClient是client与MDS通信的唯一窗口
class MDSClient : public MDSClientBase,
                  public std::enable_shared_from_this<MDSClient> {
 public:
    explicit MDSClient(const std::string &metricPrefix = "");

    virtual ~MDSClient();

    LIBCURVE_ERROR Initialize(const MetaServerOption &metaopt);

    /**
     * 创建文件
     * @param: context创建文件信息
     * @return: 成功返回LIBCURVE_ERROR::OK
     *          文件已存在返回LIBCURVE_ERROR::EXIST
     *          否则返回LIBCURVE_ERROR::FAILED
     *          如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     */
    LIBCURVE_ERROR CreateFile(const CreateFileContext& context);
    /**
     * open file
     * @param: filename  file name
     * @param: userinfo  user info
     * @param[out]: fi  file info returned
     * @param[out]: fEpoch  file epoch info returned
     * @param[out]: lease lease of file returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR OpenFile(const std::string &filename,
                            const UserInfo_t &userinfo, FInfo_t *fi,
                            FileEpoch_t *fEpoch,
                            LeaseSession *lease);

    /**
     * 获取copysetid对应的serverlist信息并更新到metacache
     * @param: logicPoolId逻辑池信息
     * @param: csid为要获取的copyset列表
     * @param: cpinfoVec保存获取到的server信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR
    GetServerList(const LogicPoolID &logicPoolId,
                  const std::vector<CopysetID> &csid,
                  std::vector<CopysetInfo<ChunkServerID>> *cpinfoVec);

    /**
     * 获取当前mds所属的集群信息
     * @param[out]: clsctx 为要获取的集群信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetClusterInfo(ClusterContext *clsctx);

    LIBCURVE_ERROR ListPoolset(std::vector<std::string>* out);

    /**
     * Get or Alloc SegmentInfo，and update to Metacache
     * @param: allocate  ture for allocate, false for get only
     * @param: offset  segment start offset
     * @param: fi file info
     * @param: fEpoch  file epoch info
     * @param[out]: segInfo segment info returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetOrAllocateSegment(bool allocate, uint64_t offset,
                                        const FInfo_t *fi,
                                        const FileEpoch_t *fEpoch,
                                        SegmentInfo *segInfo);

    /**
     * @brief Send DeAllocateSegment request to current working MDS
     * @param fileInfo current file info
     * @param offset segment start offset
     * @return LIBCURVE_ERROR::OK means success, other value means fail
     */
    virtual LIBCURVE_ERROR DeAllocateSegment(const FInfo *fileInfo,
                                             uint64_t offset);

    /**
     * Get File Info
     * @param: filename  file name
     * @param: userinfo  user info
     * @param[out]: fi  file info returned
     * @param[out]: fEpoch  file epoch info returned
     * @return:
     * return LIBCURVE_ERROR::OK for success,
     * return LIBCURVE_ERROR::AUTHFAIL for auth fail,
     * otherwise return LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetFileInfo(const std::string &filename,
                               const UserInfo_t &userinfo,
                               FInfo_t *fi,
                               FileEpoch_t *fEpoch);

    /**
     * @brief Increase epoch and return chunkserver locations
     *
     * @param[in] filename  file name
     * @param[in] userinfo  user info
     * @param[out] fi  file info
     * @param[out] fEpoch  file epoch info
     * @param[out] csLocs  chunkserver locations
     *
     * @return LIBCURVE_ERROR::OK for success, LIBCURVE_ERROR::FAILED for fail.
     */
    LIBCURVE_ERROR IncreaseEpoch(const std::string& filename,
         const UserInfo_t& userinfo,
         FInfo_t* fi,
         FileEpoch_t *fEpoch,
         std::list<CopysetPeerInfo<ChunkServerID>> *csLocs);

    /**
     * 扩展文件
     * @param: userinfo是用户信息
     * @param: filename文件名
     * @param: newsize新的size
     */
    LIBCURVE_ERROR Extend(const std::string &filename,
                          const UserInfo_t &userinfo, uint64_t newsize);
    /**
     * 删除文件
     * @param: userinfo是用户信息
     * @param: filename待删除的文件名
     * @param: deleteforce是否强制删除而不放入垃圾回收站
     * @param: id为文件id，默认值为0，如果用户不指定该值，不会传id到mds
     */
    LIBCURVE_ERROR DeleteFile(const std::string &filename,
                              const UserInfo_t &userinfo,
                              bool deleteforce = false, uint64_t id = 0);

    /**
     * recover file
     * @param: userinfo
     * @param: filename
     * @param: fileId is inodeid，default 0
     */
    LIBCURVE_ERROR RecoverFile(const std::string &filename,
                               const UserInfo_t &userinfo, uint64_t fileId);

    /**
     * 创建版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要创建快照的文件名
     * @param: seq是出参，返回创建快照时文件的版本信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CreateSnapShot(const std::string &filename,
                                  const UserInfo_t &userinfo, uint64_t *seq);
    /**
     * 删除版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要快照的文件名
     * @param: seq是创建快照时文件的版本信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR DeleteSnapShot(const std::string &filename,
                                  const UserInfo_t &userinfo, uint64_t seq);

    /**
     * 以列表的形式获取版本号为seq的snapshot文件信息，snapif是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: snapif是出参，保存文件的基本信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR ListSnapShot(const std::string &filename,
                                const UserInfo_t &userinfo,
                                const std::vector<uint64_t> *seq,
                                std::map<uint64_t, FInfo> *snapif);
    /**
     * 获取快照的chunk信息并更新到metacache，segInfo是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: offset是文件内的偏移
     * @param: segInfo是出参，保存chunk信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetSnapshotSegmentInfo(const std::string &filename,
                                          const UserInfo_t &userinfo,
                                          uint64_t seq, uint64_t offset,
                                          SegmentInfo *segInfo);
    /**
     * 获取快照状态
     * @param: filenam文件名
     * @param: userinfo是用户信息
     * @param: seq是文件版本号信息
     * @param[out]: filestatus为快照状态
     */
    LIBCURVE_ERROR CheckSnapShotStatus(const std::string &filename,
                                       const UserInfo_t &userinfo, uint64_t seq,
                                       FileStatus *filestatus);

    /**
     * 文件接口在打开文件的时候需要与mds保持心跳，refresh用来续约
     * 续约结果将会通过LeaseRefreshResult* resp返回给调用层
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @param: resp是mds端传递过来的lease信息
     * @param[out]: lease当前文件的session信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR RefreshSession(const std::string &filename,
                                  const UserInfo_t &userinfo,
                                  const std::string &sessionid,
                                  LeaseRefreshResult *resp,
                                  LeaseSession *lease = nullptr);
    /**
     * 关闭文件，需要携带sessionid，这样mds端会在数据库删除该session信息
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @return:
     * 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CloseFile(const std::string &filename,
                             const UserInfo_t &userinfo,
                             const std::string &sessionid);

    /**
     * @brief 创建clone文件
     * @detail
     *  - 若是clone，sn重置为初始值
     *  - 若是recover，sn不变
     *
     * @param source 克隆源文件名
     * @param:destination clone目标文件名
     * @param:userinfo 用户信息
     * @param:size 文件大小
     * @param:sn 版本号
     * @param:chunksize是创建文件的chunk大小
     * @param stripeUnit stripe size
     * @param stripeCount stripe count
     * @param[out] destFileId 创建的目标文件的Id
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CreateCloneFile(const std::string &source,
                                   const std::string &destination,
                                   const UserInfo_t &userinfo, uint64_t size,
                                   uint64_t sn, uint32_t chunksize,
                                   uint64_t stripeUnit, uint64_t stripeCount,
                                   const std::string& poolset,
                                   FInfo *fileinfo);

    /**
     * @brief 通知mds完成Clone Meta
     *
     * @param:destination 目标文件
     * @param:userinfo用户信息
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CompleteCloneMeta(const std::string &destination,
                                     const UserInfo_t &userinfo);

    /**
     * @brief 通知mds完成Clone Chunk
     *
     * @param:destination 目标文件
     * @param:userinfo用户信息
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CompleteCloneFile(const std::string &destination,
                                     const UserInfo_t &userinfo);

    /**
     * @brief 通知mds完成Clone Meta
     *
     * @param: filename 目标文件
     * @param: filestatus为要设置的目标状态
     * @param: userinfo用户信息
     * @param: fileId为文件ID信息，非必填
     *
     * @return 错误码
     */
    LIBCURVE_ERROR SetCloneFileStatus(const std::string &filename,
                                      const FileStatus &filestatus,
                                      const UserInfo_t &userinfo,
                                      uint64_t fileID = 0);

    /**
     * @brief 重名文件
     *
     * @param:userinfo 用户信息
     * @param:originId 被恢复的原始文件Id
     * @param:destinationId 克隆出的目标文件Id
     * @param:origin 被恢复的原始文件名
     * @param:destination 克隆出的目标文件
     *
     * @return 错误码
     */
    LIBCURVE_ERROR RenameFile(const UserInfo_t &userinfo,
                              const std::string &origin,
                              const std::string &destination,
                              uint64_t originId = 0,
                              uint64_t destinationId = 0);

    /**
     * 变更owner
     * @param: filename待变更的文件名
     * @param: newOwner新的owner信息
     * @param: userinfo执行此操作的user信息，只有root用户才能执行变更
     * @return: 成功返回0，
     *          否则返回LIBCURVE_ERROR::FAILED,LIBCURVE_ERROR::AUTHFAILED等
     */
    LIBCURVE_ERROR ChangeOwner(const std::string &filename,
                               const std::string &newOwner,
                               const UserInfo_t &userinfo);

    /**
     * 枚举目录内容
     * @param: userinfo是用户信息
     * @param: dirpath是目录路径
     * @param[out]: filestatVec当前文件夹内的文件信息
     */
    LIBCURVE_ERROR Listdir(const std::string &dirpath,
                           const UserInfo_t &userinfo,
                           std::vector<FileStatInfo> *filestatVec);

    /**
     * 向mds注册client metric监听的地址和端口
     * @param: ip客户端ip
     * @param: dummyServerPort为监听端口
     * @return: 成功返回0，
     *          否则返回LIBCURVE_ERROR::FAILED,LIBCURVE_ERROR::AUTHFAILED等
     */
    LIBCURVE_ERROR Register(const std::string &ip, uint16_t port);

    /**
     * 获取chunkserver信息
     * @param[in] addr chunkserver地址信息
     * @param[out] chunkserverInfo 待获取的信息
     * @return：成功返回ok
     */
    LIBCURVE_ERROR
    GetChunkServerInfo(const PeerAddr &addr,
                       CopysetPeerInfo<ChunkServerID> *chunkserverInfo);

    /**
     * 获取server上所有chunkserver的id
     * @param[in]: ip为server的ip地址
     * @param[out]: csIds用于保存chunkserver的id
     * @return: 成功返回LIBCURVE_ERROR::OK，失败返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR ListChunkServerInServer(const std::string &ip,
                                           std::vector<ChunkServerID> *csIds);

    /**
     * 析构，回收资源
     */
    void UnInitialize();

    /**
     * 将mds侧错误码对应到libcurve错误码
     * @param: statecode为mds一侧错误码
     * @param[out]: 出参errcode为libcurve一侧的错误码
     */
    void MDSStatusCode2LibcurveError(const ::curve::mds::StatusCode &statcode,
                                     LIBCURVE_ERROR *errcode);

    LIBCURVE_ERROR ReturnError(int retcode);

 private:
    // 初始化标志，放置重复初始化
    bool inited_ = false;

    // 当前模块的初始化option配置
    MetaServerOption metaServerOpt_;

    // client与mds通信的metric统计
    MDSClientMetric mdsClientMetric_;

    RPCExcutorRetryPolicy rpcExcutor_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_H_
