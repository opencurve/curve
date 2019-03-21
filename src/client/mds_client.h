/*
 * Project: curve
 * File Created: Monday, 18th February 2019 6:25:17 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_MDS_CLIENT_H
#define CURVE_MDS_CLIENT_H

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <vector>
#include <string>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/libcurve_define.h"

// TODO(tongguangxun) : 添加mds端RPC重试逻辑，解耦重试逻辑与正常RPC收发逻辑
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
    LIBCURVE_ERROR Initialize(UserInfo_t userinfo, MetaServerOption_t metaaddr);
    /**
     * 创建文件
     * @param: filename创建文件的文件名
     * @param: size文件长度
     * @return: 成功返回LIBCURVE_ERROR::OK
     *          文件已存在返回LIBCURVE_ERROR::EXIST
     *          否则返回LIBCURVE_ERROR::FAILED
     *          如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     */
    LIBCURVE_ERROR CreateFile(std::string filename,
                                size_t size);
    /**
     * 打开文件
     * @param: 是文件名
     * @param: fi是出参，保存该文件信息
     * @param: lease是出参，携带该文件对应的lease信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR OpenFile(std::string name,
                            FInfo_t* fi,
                            LeaseSession* lease);
    /**
     * 获取copysetid对应的serverlist信息并更新到metacache
     * @param: logicPoolId逻辑池信息
     * @param: csid为要获取的copyset列表
     * @param: mc是要更新的metacache
     * @return: 成功返回LIBCURVE_ERROR::OK,否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetServerList(const LogicPoolID &logicPoolId,
                            const std::vector<CopysetID>& csid,
                            MetaCache* mc);
    /**
     * 获取segment的chunk信息，并更新到Metacache
     * @param: allocate为true的时候mds端发现不存在就分配，为false的时候不分配
     * @param: offset为文件整体偏移
     * @param: fi是当前文件的基本信息
     * @param: mc是要更新的metacache
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetOrAllocateSegment(bool allocate,
                            LogicalPoolCopysetIDInfo* lpcsIDInfo,
                            uint64_t offset,
                            const FInfo_t* fi,
                            MetaCache* mc);
    /**
     * 获取文件信息，fi是出参
     * @param: allocate为true的时候mds端发现不存在就分配，为false的时候不分配
     * @param: fi是出参，保存文件基本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetFileInfo(std::string filename,
                            FInfo_t* fi);
    /**
     * 创建版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要创建快照的文件名
     * @param: seq是出参，返回创建快照时文件的版本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CreateSnapShot(std::string filename,
                            UserInfo_t userinfo,
                            uint64_t* seq);
    /**
     * 删除版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要快照的文件名
     * @param: seq是创建快照时文件的版本信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR DeleteSnapShot(std::string filename,
                            UserInfo_t userinfo,
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
    LIBCURVE_ERROR GetSnapShot(std::string filename,
                            UserInfo_t userinfo,
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
    LIBCURVE_ERROR ListSnapShot(std::string filename,
                            UserInfo_t userinfo,
                            const std::vector<uint64_t>* seq,
                            std::vector<FInfo*>* snapif);
    /**
     * 获取快照的chunk信息并更新到metacache，segInfo是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: offset是文件内的偏移
     * @param: segInfo是出参，保存chunk信息
     * @param: mc是待更新的metacach指针
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR GetSnapshotSegmentInfo(std::string filename,
                            UserInfo_t userinfo,
                            LogicalPoolCopysetIDInfo* lpcsIDInfo,
                            uint64_t seq,
                            uint64_t offset,
                            SegmentInfo *segInfo,
                            MetaCache* mc);
    /**
     * 获取快照状态
     * @param: filenam文件名
     * @param: userinfo是用户信息
     * @param: seq是文件版本号信息
     */
    LIBCURVE_ERROR CheckSnapShotStatus(std::string filename,
                            UserInfo_t userinfo,
                            uint64_t seq);

    /**
     * 文件接口在打开文件的时候需要与mds保持心跳，refresh用来续约
     * 续约结果将会通过leaseRefreshResult* resp返回给调用层
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @param: date是当前时间
     * @param: signature是签名信息
     * @param: resp是mds端传递过来的lease信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR RefreshSession(std::string filename,
                            std::string sessionid,
                            uint64_t date,
                            std::string signature,
                            leaseRefreshResult* resp);
    /**
     * 关闭文件，需要携带sessionid，这样mds端会在数据库删除该session信息
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @return: 成功返回LIBCURVE_ERROR::OK,如果认证失败返回LIBCURVE_ERROR::AUTHFAIL，
     *          否则返回LIBCURVE_ERROR::FAILED
     */
    LIBCURVE_ERROR CloseFile(std::string filename, std::string sessionid);
    /**
     * 析构，回收资源
     */
    void UnInitialize();

 private:
    // 初始化标志，放置重复初始化
    bool            inited_;

    // client与mds通信的rpc channel
    brpc::Channel*  channel_;

    // 当前模块的初始化option配置
    MetaServerOption_t metaServerOpt_;

    // 与mds通信时携带的user信息
    UserInfo_t userinfo_;
};
}   // namespace client
}   // namespace curve

#endif  // !CURVE_MDS_CLIENT_H
