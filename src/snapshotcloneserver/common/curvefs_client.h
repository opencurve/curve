/*************************************************************************
	> File Name: curvefs_client.h
	> Author:
	> Created Time: Wed Nov 21 11:33:46 2018
    > Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_


#include<string>
#include <vector>
#include "proto/nameserver2.pb.h"
#include "proto/chunk.pb.h"

#include "src/client/client_common.h"
#include "src/client/libcurve_snapshot.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/config.h"

using ::curve::client::SegmentInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::CopysetID;
using ::curve::client::ChunkID;
using ::curve::client::ChunkInfoDetail;
using ::curve::client::ChunkIDInfo;
using ::curve::client::FInfo;
using ::curve::client::FileStatus;

namespace curve {
namespace snapshotcloneserver {

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
    // TODO(xuchaojie): 后续考虑支持用户登录的方式，接口不传user。

    /**
     * @brief client 资源回收
     *
     * @return 错误码
     */
    virtual int UnInit() = 0;

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
        uint64_t *seq) = 0;

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
     *
     * @return 错误码
     */
    virtual int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf) = 0;
    /**
     * @brief 删除此次转储时产生的或者历史遗留的快照
     *        如果转储过程中没有产生快照，则修改chunk的correctedSn
     *
     * @param cidinfo chunk ID信息
     * @param correctedSeq chunk快照不存在时需要修正的版本号
     *
     * @return 错误码
     */
    virtual int DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
        uint64_t correctedSeq) = 0;

    /**
     * 获取快照状态
     * @param: userinfo是用户信息
     * @param: filenam文件名
     * @param: seq是文件版本号信息
     */
    virtual int CheckSnapShotStatus(std::string filename,
                                std::string user,
                                uint64_t seq) = 0;

    /**
     * @brief 获取chunk的版本号信息
     *
     * @param cidinfo chunk ID 信息
     * @param chunkInfo chunk详细信息
     *
     * @return 错误码
     */
    virtual int GetChunkInfo(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo) = 0;

    /**
     * @brief 创建clone文件
     * @detail
     *  - 若是clone，sn重置为初始值
     *  - 若是recover，sn不变
     *
     * @param filename clone目标文件名
     * @param user 用户信息
     * @param size 文件大小
     * @param sn 版本号
     * @param chunkSize chunk大小
     * @param[out] fileInfo 文件信息
     *
     * @return 错误码
     */
    virtual int CreateCloneFile(
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
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
     *
     * @return 错误码
     */
    virtual int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize) = 0;


    /**
     * @brief 实际恢复chunk数据
     *
     * @param chunkidinfo chunkidinfo
     * @param offset 偏移
     * @param len 长度
     *
     * @return 错误码
     */
    virtual int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len) = 0;

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
        const FInfo* fileInfo,
        const std::string user,
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
};

class CurveFsClientImpl : public CurveFsClient {
 public:
    CurveFsClientImpl() {}
    virtual ~CurveFsClientImpl() {}

    // 以下接口定义见CurveFsClient接口注释
    int Init(const CurveClientOptions &options) override;

    int UnInit() override;

    int CreateSnapshot(const std::string &filename,
        const std::string &user,
        uint64_t *seq) override;

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
                        void *buf) override;

    int DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
        uint64_t seq) override;

    int CheckSnapShotStatus(std::string filename,
                            std::string user,
                            uint64_t seq) override;

    int GetChunkInfo(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo) override;

    int CreateCloneFile(
        const std::string &filename,
        const std::string &user,
        uint64_t size,
        uint64_t sn,
        uint32_t chunkSize,
        FInfo* fileInfo) override;

    int CreateCloneChunk(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize) override;

    int RecoverChunk(
        const ChunkIDInfo &chunkidinfo,
        uint64_t offset,
        uint64_t len) override;

    int CompleteCloneMeta(
        const std::string &filename,
        const std::string &user) override;

    int CompleteCloneFile(
        const std::string &filename,
        const std::string &user) override;

    int GetFileInfo(
        const std::string &filename,
        const std::string &user,
        FInfo* fileInfo) override;

    int GetOrAllocateSegmentInfo(
        bool allocate,
        uint64_t offset,
        const FInfo* fileInfo,
        const std::string user,
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
        uint64_t fileId) override {
        // TODO(xuchaojie): fix it
        return -1;
    }

 private:
    ::curve::client::SnapshotClient client_;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CURVEFS_CLIENT_H_

