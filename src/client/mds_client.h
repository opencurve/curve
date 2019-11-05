/*
 * Project: curve
 * File Created: Monday, 18th February 2019 6:25:17 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_MDS_CLIENT_H_
#define SRC_CLIENT_MDS_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/errno.pb.h>
#include <bthread/mutex.h>

#include <map>
#include <vector>
#include <string>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/libcurve_define.h"
#include "src/client/metacache_struct.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/concurrent.h"
#include "src/client/mds_client_base.h"
#include "src/client/client_metric.h"

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::Authenticator;

namespace curve {
namespace client {
class MetaCache;
struct leaseRefreshResult;
// MDSClient是client与MDS通信的唯一窗口
class MDSClient {
 public:
    MDSClient();
    ~MDSClient() = default;
    /**
     * 初始化函数
     * @param: metaopt为mdsclient的配置信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR Initialize(const MetaServerOption_t& metaopt);
    /**
     * 创建文件
     * @param: filename创建文件的文件名
     * @param: userinfo为user信息
     * @param: size文件长度
     * @param: normalFile表示创建的是普通文件还是目录文件，如果是目录则忽略size
     * @return: 成功返回LIBCURVE_ERROR::OK
     *          文件已存在返回LIBCURVE_ERROR::EXIST
     *          否则返回LIBCURVE_ERROR::FAILED
     *          如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     */
    LIBCURVE_ERROR CreateFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            size_t size = 0,
                            bool normalFile = true);
    /**
     * 打开文件
     * @param: filename是文件名
     * @param: userinfo为user信息
     * @param: fi是出参，保存该文件信息
     * @param: lease是出参，携带该文件对应的lease信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR OpenFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            FInfo_t* fi,
                            LeaseSession* lease);
    /**
     * 获取copysetid对应的serverlist信息并更新到metacache
     * @param: logicPoolId逻辑池信息
     * @param: csid为要获取的copyset列表
     * @param: cpinfoVec保存获取到的server信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetServerList(const LogicPoolID &logicPoolId,
                            const std::vector<CopysetID>& csid,
                            std::vector<CopysetInfo_t>* cpinfoVec);

   /**
    * 获取当前mds所属的集群信息
    * @param[out]: clsctx 为要获取的集群信息
    * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
    */
    LIBCURVE_ERROR GetClusterInfo(ClusterContext* clsctx);

    /**
     * 获取segment的chunk信息，并更新到Metacache
     * @param: allocate为true的时候mds端发现不存在就分配，为false的时候不分配
     * @param: offset为文件整体偏移
     * @param: fi是当前文件的基本信息
     * @param[out]: segInfoh获取当前segment的内部chunk信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetOrAllocateSegment(bool allocate,
                            uint64_t offset,
                            const FInfo_t* fi,
                            SegmentInfo *segInfo);
    /**
     * 获取文件信息，fi是出参
     * @param: filename是文件名
     * @param: userinfo为user信息
     * @param: fi是出参，保存文件基本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetFileInfo(const std::string& filename,
                            const UserInfo_t& userinfo,
                            FInfo_t* fi);
    /**
     * 扩展文件
     * @param: userinfo是用户信息
     * @param: filename文件名
     * @param: newsize新的size
     */
    LIBCURVE_ERROR Extend(const std::string& filename,
                          const UserInfo_t& userinfo,
                          uint64_t newsize);
    /**
     * 删除文件
     * @param: userinfo是用户信息
     * @param: filename待删除的文件名
     * @param: deleteforce是否强制删除而不放入垃圾回收站
     * @param: id为文件id，默认值为0，如果用户不指定该值，不会传id到mds
     */
    LIBCURVE_ERROR DeleteFile(const std::string& filename,
                              const UserInfo_t& userinfo,
                              bool deleteforce = false,
                              uint64_t id = 0);
    /**
     * 创建版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要创建快照的文件名
     * @param: seq是出参，返回创建快照时文件的版本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CreateSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t* seq);
    /**
     * 删除版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要快照的文件名
     * @param: seq是创建快照时文件的版本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR DeleteSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq);

    /**
     * 以列表的形式获取版本号为seq的snapshot文件信息，snapif是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: snapif是出参，保存文件的基本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR ListSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::vector<uint64_t>* seq,
                            std::map<uint64_t, FInfo>* snapif);
    /**
     * 获取快照的chunk信息并更新到metacache，segInfo是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: offset是文件内的偏移
     * @param: segInfo是出参，保存chunk信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetSnapshotSegmentInfo(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq,
                            uint64_t offset,
                            SegmentInfo *segInfo);
    /**
     * 获取快照状态
     * @param: filenam文件名
     * @param: userinfo是用户信息
     * @param: seq是文件版本号信息
     * @param[out]: filestatus为快照状态
     */
    LIBCURVE_ERROR CheckSnapShotStatus(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq,
                            FileStatus* filestatus);

    /**
     * 文件接口在打开文件的时候需要与mds保持心跳，refresh用来续约
     * 续约结果将会通过leaseRefreshResult* resp返回给调用层
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @param: resp是mds端传递过来的lease信息
     * @param[out]: lease当前文件的session信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR RefreshSession(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::string& sessionid,
                            leaseRefreshResult* resp);
    /**
     * 关闭文件，需要携带sessionid，这样mds端会在数据库删除该session信息
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CloseFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::string& sessionid);

    /**
     * @brief 创建clone文件
     * @detail
     *  - 若是clone，sn重置为初始值
     *  - 若是recover，sn不变
     *
     * @param:destination clone目标文件名
     * @param:userinfo 用户信息
     * @param:size 文件大小
     * @param:sn 版本号
     * @param:chunksize是创建文件的chunk大小
     * @param[out] destFileId 创建的目标文件的Id
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CreateCloneFile(const std::string &destination,
                            const UserInfo_t& userinfo,
                            uint64_t size,
                            uint64_t sn,
                            uint32_t chunksize,
                            FInfo* fileinfo);

    /**
     * @brief 通知mds完成Clone Meta
     *
     * @param:destination 目标文件
     * @param:userinfo用户信息
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CompleteCloneMeta(const std::string &destination,
                            const UserInfo_t& userinfo);

    /**
     * @brief 通知mds完成Clone Chunk
     *
     * @param:destination 目标文件
     * @param:userinfo用户信息
     *
     * @return 错误码
     */
    LIBCURVE_ERROR CompleteCloneFile(const std::string &destination,
                            const UserInfo_t& userinfo);

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
                                    const FileStatus& filestatus,
                                    const UserInfo_t& userinfo,
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
    LIBCURVE_ERROR RenameFile(const UserInfo_t& userinfo,
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
    LIBCURVE_ERROR ChangeOwner(const std::string& filename,
                              const std::string& newOwner,
                              const UserInfo_t& userinfo);

     /**
      * 枚举目录内容
      * @param: userinfo是用户信息
      * @param: dirpath是目录路径
      * @param[out]: filestatVec当前文件夹内的文件信息
      */
    LIBCURVE_ERROR Listdir(const std::string& dirpath,
                              const UserInfo_t& userinfo,
                              std::vector<FileStatInfo>* filestatVec);

     /**
      * 向mds注册client metric监听的地址和端口
      * @param: ip客户端ip
      * @param: dummyServerPort为监听端口
      * @return: 成功返回0，
      *          否则返回LIBCURVE_ERROR::FAILED,LIBCURVE_ERROR::AUTHFAILED等
      */
    LIBCURVE_ERROR Register(const std::string& ip,
                            uint16_t port);

     /**
      * 析构，回收资源
      */
    void UnInitialize();

    // 测试使用
    MDSClientMetric_t* GetMetric() {
       return &mdsClientMetric_;
    }

 private:
    /**
     * 切换MDS链接
     * @param[in][out]: mdsAddrleft代表当前RPC调用还有多少个mdsserver可以尝试，内部
     *                  每重试一个mds addr就将该值减一
     * @return: 成功则true,否则false
     */
    bool ChangeMDServer(int* mdsAddrleft);

    /**
     * 当mds以集群方式部署时，其有多个server节点。client在于mds通信过程中如果
     * RPC超时，首先选择在当前mds server上进行超时重试。如果超时重试次数超过设置
     * 的上限范围，就从配置的集群server中切换sever节点进行重试。
     * libcurve在调用mds服务的时候，有些接口是在用户一侧是同步接口，例如Open、
     * GetFileInfo操作，这些操作不需要无限重试，只需要在synchronizeRPCRetryTime
     * 以内不成功就向上返回错误。而对于GetSegment和GetServerList接口则是在IO路径
     * 上调用的，因为IO路径上如果出现错误就一致尝试重试，重试次数很大。所以为了区分两
     * 种类型的RPC调用，需要设置sync标志，在除GetSegment和GetServerList接口外的
     * RPC调用都应该设置sync = true。
     * sync主要用来判断当前RPC重试次数是否需要进行切换server，当sync = true时，重试
     * 次数达到synchronizeRPCRetryTime时就要切换server了。当sync = false时，重试
     * 次数达到rpcRetryTimes时就要切换server。
     * 因为这部分逻辑在所有的RPC接口里都会用到，所以为了复用代码减少重复代码，把这部分逻辑
     * 单独抽出来。
     * @param[in][out] :retrycount是入参，也是出参，更新重试的次数，
     *        如果这个次数超过设置的规定次数，那么就切换leader重试，并将该值置0
     * @param: mdsAddrleft代表当前RPC调用还有多少个mdsserver可以尝试，如果还有server
     *          可以重试，就调用ChangeMDServer切换server重试。
     * @param: sync代表当前调用是否为同步调用，根据sync值选择重试次数
     * @return: 达到重试次数且切换server失败返回false， 否则返回true
     */
    bool UpdateRetryinfoOrChangeServer(int* retrycount,
                                       int* mdsAddrleft,
                                       bool sync = true);

    /**
     * 将mds侧错误码对应到libcurve错误码
     * @param: statecode为mds一侧错误码
     * @param[out]: 出参errcode为libcurve一侧的错误码
     */
    void MDSStatusCode2LibcurveError(const ::curve::mds::StatusCode& statcode,
                                     LIBCURVE_ERROR* errcode);

 private:
    // 初始化标志，放置重复初始化
    bool            inited_;

    // brpc提供的mutex锁
    mutable bthread::Mutex mutex_;

    // client与mds通信的rpc channel
    brpc::Channel*  channel_;

    // 当前模块的初始化option配置
    MetaServerOption_t metaServerOpt_;

    // 记录上一次重试过的leader信息, 该索引对应metaServerOpt_里leader addr的索引
    int lastWorkingMDSAddrIndex_;

    // 读写锁，在切换MDS地址的时候需要暂停其他线程的RPC调用
    RWLock rwlock_;

    // client与mds通信的metric统计
    MDSClientMetric_t mdsClientMetric_;

    // MDSClientBase是真正的rpc发送逻辑
    // MDSClient是在RPC上层的一些业务逻辑封装，比如metric，或者重试逻辑
    MDSClientBase mdsClientBase_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_H_
