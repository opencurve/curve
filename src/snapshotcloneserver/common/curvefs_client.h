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

/*************************************************************************
	> File Name: curvefs_client.h
	> Author:
	> Created Time: Wed Nov 21 11:33:46 2018
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_


#include<string>
#include <vector>
#include <memory>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "proto/nameserver2.pb.h"
#include "proto/chunk.pb.h"

#include "src/client/client_common.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/libcurve_file.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/common/timeutility.h"

using ::curve::client::SegmentInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::CopysetID;
using ::curve::client::ChunkID;
using ::curve::client::ChunkInfoDetail;
using ::curve::client::ChunkIDInfo;
using ::curve::client::FInfo;
using ::curve::client::FileStatus;
using ::curve::client::SnapCloneClosure;
using ::curve::client::UserInfo;
using ::curve::client::SnapshotClient;
using ::curve::client::FileClient;

namespace curve {
namespace snapshotcloneserver {

using RetryMethod = std::function<int()>;
using RetryCondition = std::function<bool(int)>;

class RetryHelper {
 public:
    RetryHelper(const RetryMethod &retryMethod,
        const RetryCondition &condition) {
        retryMethod_ = retryMethod;
        condition_ = condition;
    }

    int RetryTimeSecAndReturn(
        uint64_t retryTimeSec,
        uint64_t retryIntervalMs) {
        int ret = -LIBCURVE_ERROR::FAILED;
        uint64_t startTime = TimeUtility::GetTimeofDaySec();
        uint64_t nowTime = startTime;
        do {
            ret = retryMethod_();
            if (!condition_(ret)) {
                return ret;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryIntervalMs));
            nowTime = TimeUtility::GetTimeofDaySec();
        } while (nowTime - startTime < retryTimeSec);
        return ret;
    }

 private:
    RetryMethod  retryMethod_;
    RetryCondition condition_;
};


int LibCurveErrToSnapshotCloneErr(int errCode);

class CurveFsClient {
 public:
    CurveFsClient() {}
    virtual ~CurveFsClient() {}

    /**
     * @brief client 初始化
     *
     * @return 错误码
     */
    virtual int Init(const CurveClientOptions &options) = 0;

    /**
     * @brief client 资源回收
     *
     * @return 错误码
     */
    virtual int UnInit() = 0;

    virtual int CreateFile(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset) = 0;

    virtual int DeleteFile(const std::string &file,
        const std::string &user) = 0;

    /**
     * @brief 创建快照
     *
     * @param filename 文件名
     * @param user  用户信息
     * @param[out] seq 快照版本号
     *
     * @return 错误码
     */
    virtual int CreateSnapshot(const std::string &filename,
        const std::string &user,
        FInfo* snapInfo) = 0;

    /**
     * @brief 删除快照
     *
     * @param filename 文件名
     * @param user 用户信息
     * @param seq 快照版本号
     *
     * @return 错误码
     */
    virtual int DeleteSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq) = 0;

    /**
     * @brief 获取快照文件信息
     *
     * @param filename 文件名
     * @param user 用户名
     * @param seq 快照版本号
     * @param[out] snapInfo 快照文件信息
     *
     * @return 错误码
     */
    virtual int GetSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq, FInfo* snapInfo) = 0;

    /**
     * @brief 查询快照文件segment信息
     *
     * @param filename 文件名
     * @param user 用户信息
     * @param seq 快照版本号
     * @param offset 偏移值
     * @param segInfo segment信息
     *
     * @return 错误码
     */
    virtual int GetSnapshotSegmentInfo(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) = 0;

    /**
     * @brief 读取snapshot chunk的数据
     *
     * @param cidinfo chunk ID 信息
     * @param seq 快照版本号
     * @param offset 偏移值
     * @param len 长度
     * @param[out] buf buffer指针
     * @param: scc是异步回调
     *
     * @return 错误码
     */
    virtual int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf,
                        SnapCloneClosure* scc) = 0;

    /**
     * 获取快照状态
     * @param: userinfo是用户信息
     * @param: filenam文件名
     * @param: seq是文件版本号信息
     * @param: filestatus 快照文件状态
     * @param: progress 快照处于deleting状态时的删除进度（0-100）
     */
    virtual int CheckSnapShotStatus(std::string filename,
                                std::string user,
                                uint64_t seq,
                                FileStatus* filestatus,
                                uint32_t* progress = nullptr) = 0;

    /**
     * @brief 获取chunk的版本号信息
     *
     * @param cidinfo chunk ID 信息
     * @param chunkInfo chunk详细信息
     *
     * @return 错误码
     */
    virtual int GetChunkInfo(const ChunkIDInfo &cidinfo,
        ChunkInfoDetail *chunkInfo) = 0;

    /**
     * @brief 创建clone文件
     * @detail
     *  - 若是clone，sn重置为初始值
     *  - 若是recover，sn不变
     *
     * @param source clone源文件名
     * @param filename clone目标文件名
     * @param user 用户信息
     * @param size 文件大小
     * @param sn 版本号
     * @param chunkSize chunk大小
     * @param stripeUnit stripe size
     * @param stripeCount stripe count
     * @param[out] fileInfo 文件信息
     *
     * @return 错误码
     */
    virtual int CreateCloneFile(
        const std::string &source,
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string& poolset,
        FInfo* fileInfo) = 0;

    /**
     * @brief lazy 创建clone chunk
     * @detail
     *  - location的格式定义为 A@B的形式。
     *  - 如果源数据在s3上，则location格式为uri@s3，uri为实际chunk对象的地址；
     *  - 如果源数据在curvefs上，则location格式为/filename/chunkindex@cs
     *
     * @param location 数据源的url
     * @param chunkidinfo 目标chunk
     * @param sn chunk的序列号
     * @param csn correct sn
     * @param chunkSize chunk的大小
     * @param: scc是异步回调
     *
     * @return 错误码
     */
    virtual int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize,
        SnapCloneClosure* scc) = 0;


    /**
     * @brief 实际恢复chunk数据
     *
     * @param chunkidinfo chunkidinfo
     * @param offset 偏移
     * @param len 长度
     * @param: scc是异步回调
     *
     * @return 错误码
     */
    virtual int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure* scc) = 0;

    /**
     * @brief 通知mds完成Clone Meta
     *
     * @param filename 目标文件名
     * @param user 用户名
     *
     * @return 错误码
     */
    virtual int CompleteCloneMeta(
        const std::string &filename,
        const std::string &user) = 0;

    /**
     * @brief 通知mds完成Clone Chunk
     *
     * @param filename 目标文件名
     * @param user 用户名
     *
     * @return 错误码
     */
    virtual int CompleteCloneFile(
        const std::string &filename,
        const std::string &user) = 0;

    /**
     * @brief 设置clone文件状态
     *
     * @param filename 文件名
     * @param filestatus 要设置的目标状态
     * @param user 用户名
     *
     * @return 错误码
     */
    virtual int SetCloneFileStatus(
        const std::string &filename,
        const FileStatus& filestatus,
        const std::string &user) = 0;

    /**
     * @brief 获取文件信息
     *
     * @param filename 文件名
     * @param user 用户名
     * @param[out] fileInfo 文件信息
     *
     * @return 错误码
     */
    virtual int GetFileInfo(
        const std::string &filename,
        const std::string &user,
        FInfo* fileInfo) = 0;

    /**
     * @brief 查询或分配文件segment信息
     *
     * @param allocate 是否分配
     * @param offset 偏移值
     * @param fileInfo 文件信息
     * @param user 用户名
     * @param segInfo segment信息
     *
     * @return 错误码
     */
    virtual int GetOrAllocateSegmentInfo(
        bool allocate,
        uint64_t offset,
        FInfo* fileInfo,
        const std::string &user,
        SegmentInfo *segInfo) = 0;

    /**
     * @brief 为recover rename复制的文件
     *
     * @param user 用户信息
     * @param originId 被恢复的原始文件Id
     * @param destinationId 克隆出的目标文件Id
     * @param origin 被恢复的原始文件名
     * @param destination 克隆出的目标文件
     *
     * @return 错误码
     */
    virtual int RenameCloneFile(
            const std::string &user,
            uint64_t originId,
            uint64_t destinationId,
            const std::string &origin,
            const std::string &destination) = 0;


    /**
     * @brief 删除文件
     *
     * @param fileName 文件名
     * @param user 用户名
     * @param fileId 删除文件的inodeId
     *
     * @return 错误码
     */
    virtual int DeleteFile(
            const std::string &fileName,
            const std::string &user,
            uint64_t fileId) = 0;

    virtual int StatFile(const std::string &file,
        const std::string &user,
        FileStatInfo *statInfo) = 0;

    virtual int ListDir(const std::string &dir,
        const std::string &user,
        std::vector<FileStatInfo> *fileStatInfos) = 0;

    /**
     * @brief 创建目录
     *
     * @param dirpath 目录名
     * @param user 用户名
     *
     * @return 错误码
     */
    virtual int Mkdir(const std::string& dirpath,
        const std::string &user) = 0;

    /**
     * @brief 变更文件的owner
     *
     * @param filename 文件名
     * @param newOwner 新的owner
     *
     * @return 错误码
     */
    virtual int ChangeOwner(const std::string& filename,
        const std::string& newOwner) = 0;

    /**
     * @brief clone
     *
     * @param file  source volume path
     * @param snapshotName  source snapshot name
     * @param user  user
     * @param destination  the destination volume of clone
     * @param poolset  the poolset of destination volume
     * @param finfo  the file info of destination volume
     *
     * @return error code
     */
    virtual int Clone(const std::string &snapPath,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        FInfo* finfo) = 0;

    /**
     * @brief flatten a file
     *
     * @param file  file to flatten
     * @param user  user of the file to flatten
     *
     * @return  error code
     */
    virtual int Flatten(const std::string &file,
        const std::string &user) = 0;

    virtual int QueryFlattenStatus(const std::string &file,
        const std::string &user,
        FileStatus* filestatus,
        uint32_t* progress) = 0;

    virtual int ProtectSnapshot(const std::string &snapPath,
        const std::string user) = 0;

    virtual int UnprotectSnapshot(const std::string &snapPath,
        const std::string user) = 0;
};

constexpr char kSnapPathSeprator[] = "@";

inline std::string MakeSnapshotPath(const std::string &filePath,
    const std::string &snapName) {
    return filePath + kSnapPathSeprator + snapName;
}

class CurveFsClientImpl : public CurveFsClient {
 public:
    CurveFsClientImpl(std::shared_ptr<SnapshotClient> snapClient,
        std::shared_ptr<FileClient> fileClient) :
        snapClient_(snapClient), fileClient_(fileClient) {}
    virtual ~CurveFsClientImpl() {}

    // 以下接口定义见CurveFsClient接口注释
    int Init(const CurveClientOptions &options) override;

    int UnInit() override;

    int CreateFile(const std::string &file,
        const std::string &user,
        uint64_t size,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string &poolset) override;

    int DeleteFile(const std::string &file,
        const std::string &user) override;

    int StatFile(const std::string &file,
        const std::string &user,
        FileStatInfo *statInfo) override;

    int ListDir(const std::string &dir,
        const std::string &user,
        std::vector<FileStatInfo> *fileStatInfos) override;

    int CreateSnapshot(const std::string &filename,
        const std::string &user,
        FInfo* snapInfo) override;

    int DeleteSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq) override;

    int GetSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        FInfo* snapInfo) override;

    int GetSnapshotSegmentInfo(const std::string &filename,
        const std::string &user,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) override;

    int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        char *buf,
                        SnapCloneClosure* scc) override;

    int CheckSnapShotStatus(std::string filename,
                            std::string user,
                            uint64_t seq,
                            FileStatus* filestatus,
                            uint32_t* progress = nullptr) override;

    int GetChunkInfo(const ChunkIDInfo &cidinfo,
        ChunkInfoDetail *chunkInfo) override;

    int CreateCloneFile(
        const std::string &source,
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        uint64_t stripeUnit,
        uint64_t stripeCount,
        const std::string& poolset,
        FInfo* fileInfo) override;

    int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize,
        SnapCloneClosure* scc) override;

    int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len,
        SnapCloneClosure* scc) override;

    int CompleteCloneMeta(
        const std::string &filename,
        const std::string &user) override;

    int CompleteCloneFile(
        const std::string &filename,
        const std::string &user) override;

    int SetCloneFileStatus(
        const std::string &filename,
        const FileStatus& filestatus,
        const std::string &user) override;

    int GetFileInfo(
        const std::string &filename,
        const std::string &user,
        FInfo* fileInfo) override;

    int GetOrAllocateSegmentInfo(
        bool allocate,
        uint64_t offset,
        FInfo* fileInfo,
        const std::string &user,
        SegmentInfo *segInfo) override;

    int RenameCloneFile(
        const std::string &user,
        uint64_t originId,
        uint64_t destinationId,
        const std::string &origin,
        const std::string &destination) override;

    int DeleteFile(
        const std::string &fileName,
        const std::string &user,
        uint64_t fileId) override;

    int Mkdir(const std::string& dirpath,
        const std::string &user) override;

    int ChangeOwner(const std::string& filename,
                    const std::string& newOwner) override;

    int Clone(const std::string &snapPath,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        FInfo* finfo) override;

    int Flatten(const std::string &file,
        const std::string &user) override;

    int QueryFlattenStatus(const std::string &file,
        const std::string &user,
        FileStatus* filestatus,
        uint32_t* progress) override;

    int ProtectSnapshot(const std::string &snapPath,
        const std::string user) override;

    int UnprotectSnapshot(const std::string &snapPath,
        const std::string user) override;

 private:
    UserInfo GetUserInfo(const std::string &user) {
        if (user == mdsRootUser_) {
            return UserInfo(mdsRootUser_, mdsRootPassword_);
        } else {
            return UserInfo(user, "");
        }
    }

 private:
    std::shared_ptr<SnapshotClient> snapClient_;
    std::shared_ptr<FileClient> fileClient_;

    std::string mdsRootUser_;
    std::string mdsRootPassword_;

    uint64_t clientMethodRetryTimeSec_;
    uint64_t clientMethodRetryIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_
