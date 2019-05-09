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

#include <vector>
#include <string>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/libcurve_define.h"
#include "src/client/metacache_struct.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/timeutility.h"
#include "src/common/authenticator.h"
#include "src/common/concurrent/concurrent.h"

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
     * @param: userinfo为user信息
     * @param: metaaddr为mdsclient的配置信息
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR Initialize(MetaServerOption_t metaaddr);
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
     * 获取segment的chunk信息，并更新到Metacache
     * @param: allocate为true的时候mds端发现不存在就分配，为false的时候不分配
     * @param: userinfo为user信息
     * @param: offset为文件整体偏移
     * @param: fi是当前文件的基本信息
     * @param[out]: segInfoh获取当前segment的内部chunk信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetOrAllocateSegment(bool allocate,
                            const UserInfo_t& userinfo,
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
     * @param: id为文件id，默认值为0，如果用户不指定该值，不会传id到mds
     */
    LIBCURVE_ERROR DeleteFile(const std::string& filename,
                              const UserInfo_t& userinfo,
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
     * 获取版本号为seq的snapshot文件信息，snapif是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: snapif是出参，保存文件的基本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq,
                            FInfo* snapif);
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
                            std::vector<FInfo*>* snapif);
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
     * 析构，回收资源
     */
    void UnInitialize();

 private:
    /**
     * 切换MDS链接
     * @param[in][out]: mdsAddrleft代表当前RPC调用还有多少个mdsserver可以尝试，内部
     *                  每重试一个mds addr就将该值减一
     * @return: 成功则true,否则false
     */
    bool ChangeMDServer(int* mdsAddrleft);

     /**
     * client在于MDS通信失败时尝试重新连接，如果超过一定次数，就切换leader重试
     * @param[in][out] :retrycount是入参，也是出参，更新重试的次数，
     *        如果这个次数超过设置的规定次数，那么就切换leader重试，并将该值置0
     * @param: mdsAddrleft代表当前RPC调用还有多少个mdsserver可以尝试，如果还有server
     *          可以重试，就调用ChangeMDServer切换server重试。
     * @return: 达到重试次数且切换server失败返回false， 否则返回true
     */
    bool UpdateRetryinfoOrChangeServer(int* retrycount, int* mdsAddrleft);

    /**
     * 将mds侧错误码对应到libcurve错误码
     * @param: statecode为mds一侧错误码
     * @param[out]: 出参errcode为libcurve一侧的错误码
     */
    void MDSStatusCode2LibcurveError(const ::curve::mds::StatusCode& statcode,
                                     LIBCURVE_ERROR* errcode);

    /**
     * 为不同的request填充user信息
     * @param: request是待填充的变量指针
     */
    template <class T>
    void FillUserInfo(T* request, const UserInfo_t& userinfo) {
        uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
        request->set_owner(userinfo.owner);
        request->set_date(date);

        if (!userinfo.owner.compare("root") &&
             userinfo.password.compare("")) {
            std::string str2sig = Authenticator::GetString2Signature(date,
                                                        userinfo.owner);
            std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                         userinfo.password);
            request->set_signature(sig);
        }
    }

 private:
    // 初始化标志，放置重复初始化
    bool            inited_;

    // client与mds通信的rpc channel
    brpc::Channel*  channel_;

    // 当前模块的初始化option配置
    MetaServerOption_t metaServerOpt_;

    // 记录上一次重试过的leader信息, 该索引对应metaServerOpt_里leader addr的索引
    int lastWorkingMDSAddrIndex_;

    // 读写锁，在切换MDS地址的时候需要暂停其他线程的RPC调用
    RWLock rwlock_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_H_
