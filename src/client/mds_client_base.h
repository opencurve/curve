/*
 * Project: curve
 * File Created: Friday, 21st June 2019 10:20:49 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_MDS_CLIENT_BASE_H_
#define SRC_CLIENT_MDS_CLIENT_BASE_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>
#include <vector>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/libcurve_define.h"
#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/common/timeutility.h"
#include "src/common/authenticator.h"

using curve::common::Authenticator;

using curve::mds::OpenFileRequest;
using curve::mds::OpenFileResponse;
using curve::mds::CreateFileRequest;
using curve::mds::CreateFileResponse;
using curve::mds::CloseFileRequest;
using curve::mds::CloseFileResponse;
using curve::mds::RenameFileRequest;
using curve::mds::RenameFileResponse;
using curve::mds::ExtendFileRequest;
using curve::mds::ExtendFileResponse;
using curve::mds::DeleteFileRequest;
using curve::mds::DeleteFileResponse;
using curve::mds::DeleteFileRequest;
using curve::mds::DeleteFileResponse;
using curve::mds::GetFileInfoRequest;
using curve::mds::GetFileInfoResponse;
using curve::mds::RegistClientRequest;
using curve::mds::RegistClientResponse;
using curve::mds::DeleteSnapShotRequest;
using curve::mds::DeleteSnapShotResponse;
using curve::mds::ReFreshSessionRequest;
using curve::mds::ReFreshSessionResponse;
using curve::mds::ListDirRequest;
using curve::mds::ListDirResponse;
using curve::mds::ChangeOwnerRequest;
using curve::mds::ChangeOwnerResponse;
using curve::mds::CreateSnapShotRequest;
using curve::mds::CreateSnapShotResponse;
using curve::mds::CreateCloneFileRequest;
using curve::mds::CreateCloneFileResponse;
using curve::mds::CreateCloneFileRequest;
using curve::mds::CreateCloneFileResponse;
using curve::mds::SetCloneFileStatusRequest;
using curve::mds::SetCloneFileStatusResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::CheckSnapShotStatusRequest;
using curve::mds::CheckSnapShotStatusResponse;
using curve::mds::ListSnapShotFileInfoRequest;
using curve::mds::ListSnapShotFileInfoResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::topology::GetChunkServerListInCopySetsRequest;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::GetClusterInfoRequest;
using curve::mds::topology::GetClusterInfoResponse;

extern const char* kRootUserName;

namespace curve {
namespace client {
// MDSClientBase将所有与mds的RPC接口抽离，与业务逻辑解耦
// 这里只负责rpc的发送，具体的业务处理逻辑通过reponse和controller向上
// 返回给调用者，有调用者处理
class MDSClientBase {
 public:
    /**
     * @param: metaServerOpt为mdsclient的配置信息
     * @return: 成功0， 否则-1
     */
    int Init(const MetaServerOption_t& metaServerOpt);

    /**
     * 打开文件
     * @param: filename是文件名
     * @param: userinfo为user信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void OpenFile(const std::string& filename,
                  const UserInfo_t& userinfo,
                  OpenFileResponse* response,
                  brpc::Controller* cntl,
                  brpc::Channel* channel);
    /**
     * 创建文件
     * @param: filename创建文件的文件名
     * @param: userinfo为user信息
     * @param: size文件长度
     * @param: normalFile表示创建的是普通文件还是目录文件，如果是目录则忽略size
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void CreateFile(const std::string& filename,
                    const UserInfo_t& userinfo,
                    size_t size,
                    bool normalFile,
                    CreateFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 关闭文件，需要携带sessionid，这样mds端会在数据库删除该session信息
     * @param: filename是要续约的文件名
     * @param: userinfo为user信息
     * @param: sessionid是文件的session信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void CloseFile(const std::string& filename,
                    const UserInfo_t& userinfo,
                    const std::string& sessionid,
                    CloseFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 获取文件信息，fi是出参
     * @param: filename是文件名
     * @param: userinfo为user信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void GetFileInfo(const std::string& filename,
                    const UserInfo_t& userinfo,
                    GetFileInfoResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 创建版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要创建快照的文件名
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void CreateSnapShot(const std::string& filename,
                    const UserInfo_t& userinfo,
                    CreateSnapShotResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 删除版本号为seq的快照
     * @param: userinfo是用户信息
     * @param: filename是要快照的文件名
     * @param: seq是创建快照时文件的版本信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void DeleteSnapShot(const std::string& filename,
                    const UserInfo_t& userinfo,
                    uint64_t seq,
                    DeleteSnapShotResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 以列表的形式获取版本号为seq的snapshot文件信息，snapif是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void ListSnapShot(const std::string& filename,
                    const UserInfo_t& userinfo,
                    const std::vector<uint64_t>* seq,
                    ListSnapShotFileInfoResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 获取快照的chunk信息并更新到metacache，segInfo是出参
     * @param: filename是要快照的文件名
     * @param: userinfo是用户信息
     * @param: seq是创建快照时文件的版本信息
     * @param: offset是文件内的偏移
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void GetSnapshotSegmentInfo(const std::string& filename,
                    const UserInfo_t& userinfo,
                    uint64_t seq,
                    uint64_t offset,
                    GetOrAllocateSegmentResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 文件接口在打开文件的时候需要与mds保持心跳，refresh用来续约
     * 续约结果将会通过leaseRefreshResult* resp返回给调用层
     * @param: filename是要续约的文件名
     * @param: sessionid是文件的session信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void RefreshSession(const std::string& filename,
                    const UserInfo_t& userinfo,
                    const std::string& sessionid,
                    ReFreshSessionResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 获取快照状态
     * @param: filenam文件名
     * @param: userinfo是用户信息
     * @param: seq是文件版本号信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void CheckSnapShotStatus(const std::string& filename,
                    const UserInfo_t& userinfo,
                    uint64_t seq,
                    CheckSnapShotStatusResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 获取copysetid对应的serverlist信息并更新到metacache
     * @param: logicPoolId逻辑池信息
     * @param: copysetidvec为要获取的copyset列表
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void GetServerList(const LogicPoolID& logicalpooid,
                    const std::vector<CopysetID>& copysetidvec,
                    GetChunkServerListInCopySetsResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);

    /**
     * 获取mds对应的cluster id
     * @param[out]: response为该rpc的respoonse，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]: channel是当前与mds建立的通道
     */
    void GetClusterInfo(GetClusterInfoResponse* response,
                        brpc::Controller* cntl,
                        brpc::Channel* channel);

    /**
     * 创建clone文件
     * @param:destination clone目标文件名
     * @param:userinfo 用户信息
     * @param:size 文件大小
     * @param:sn 版本号
     * @param:chunksize是创建文件的chunk大小
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void CreateCloneFile(const std::string &destination,
                    const UserInfo_t& userinfo,
                    uint64_t size,
                    uint64_t sn,
                    uint32_t chunksize,
                    CreateCloneFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * @brief 通知mds完成Clone Meta
     * @param: filename 目标文件
     * @param: filestatus为要设置的目标状态
     * @param: userinfo用户信息
     * @param: fileId为文件ID信息，非必填
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void SetCloneFileStatus(const std::string &filename,
                    const FileStatus& filestatus,
                    const UserInfo_t& userinfo,
                    uint64_t fileID,
                    SetCloneFileStatusResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 获取segment的chunk信息，并更新到Metacache
     * @param: allocate为true的时候mds端发现不存在就分配，为false的时候不分配
     * @param: offset为文件整体偏移
     * @param: fi是当前文件的基本信息
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void GetOrAllocateSegment(bool allocate,
                    uint64_t offset,
                    const FInfo_t* fi,
                    GetOrAllocateSegmentResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * @brief 重名文件
     * @param:userinfo 用户信息
     * @param:originId 被恢复的原始文件Id
     * @param:destinationId 克隆出的目标文件Id
     * @param:origin 被恢复的原始文件名
     * @param:destination 克隆出的目标文件
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void RenameFile(const UserInfo_t& userinfo,
                    const std::string &origin,
                    const std::string &destination,
                    uint64_t originId,
                    uint64_t destinationId,
                    RenameFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 扩展文件
     * @param: userinfo是用户信息
     * @param: filename文件名
     * @param: newsize新的size
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void Extend(const std::string& filename,
                    const UserInfo_t& userinfo,
                    uint64_t newsize,
                    ExtendFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 删除文件
     * @param: userinfo是用户信息
     * @param: filename待删除的文件名
     * @param: deleteforce是否强制删除而不放入垃圾回收站
     * @param: id为文件id，默认值为0，如果用户不指定该值，不会传id到mds
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void DeleteFile(const std::string& filename,
                    const UserInfo_t& userinfo,
                    bool deleteforce,
                    uint64_t fileid,
                    DeleteFileResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 变更owner
     * @param: filename待变更的文件名
     * @param: newOwner新的owner信息
     * @param: userinfo执行此操作的user信息，只有root用户才能执行变更
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void ChangeOwner(const std::string& filename,
                    const std::string& newOwner,
                    const UserInfo_t& userinfo,
                    ChangeOwnerResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);
    /**
     * 枚举目录内容
     * @param: userinfo是用户信息
     * @param: dirpath是目录路径
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
      */
    void Listdir(const std::string& dirpath,
                    const UserInfo_t& userinfo,
                    ListDirResponse* response,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);

    /**
     * 注册client metric信息
     * @param: ip为当前client的监听地址
     * @param: port为监听端口
     * @param[out]: response为该rpc的response，提供给外部处理
     * @param[in|out]: cntl既是入参，也是出参，返回RPC状态
     * @param[in]:channel是当前与mds建立的通道
     */
    void Register(const std::string& ip,
                    uint16_t port,
                    RegistClientResponse* reponse,
                    brpc::Controller* cntl,
                    brpc::Channel* channel);

 private:
    /**
     * 为不同的request填充user信息
     * @param: request是待填充的变量指针
     */
    template <class T>
    void FillUserInfo(T* request, const UserInfo_t& userinfo) {
        uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
        request->set_owner(userinfo.owner);
        request->set_date(date);
        request->set_signature(CalcSignature(userinfo, date));
    }

    /**
     * 递增controller id并返回
     */
    inline uint64_t GetLogId() {
       return cntlID_.fetch_add(1);
    }

 private:
    inline bool IsRootUserAndHasPassword(const UserInfo& userinfo) const {
       return userinfo.owner == kRootUserName && !userinfo.password.empty();
    }

    std::string CalcSignature(const UserInfo& userinfo, uint64_t date) const;

    // controller id，用于trace整个rpc IO链路
    // 这里直接用uint64即可，在可预测的范围内，不会溢出
    std::atomic<uint64_t> cntlID_;

    // 当前模块的初始化option配置
    MetaServerOption_t metaServerOpt_;
};
}   //  namespace client
}   //  namespace curve

#endif  // SRC_CLIENT_MDS_CLIENT_BASE_H_
