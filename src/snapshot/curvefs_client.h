/*************************************************************************
	> File Name: curvefs_client.h
	> Author:
	> Created Time: Wed Nov 21 11:33:46 2018
    > Copyright (c) 2018 netease
 ************************************************************************/

#ifndef _CURVEFS_CLIENT_H
#define _CURVEFS_CLIENT_H


#include<string>
#include <vector>
#include "proto/nameserver2.pb.h"
#include "proto/chunk.pb.h"

#include "src/client/client_common.h"
#include "src/client/libcurve_snapshot.h"

using ::curve::client::SegmentInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::CopysetID;
using ::curve::client::ChunkID;
using ::curve::client::ChunkInfoDetail;
using ::curve::client::ChunkIDInfo;
using ::curve::client::FInfo;

namespace curve {
namespace snapshotserver {

class CurveFsClient {
 public:
    CurveFsClient() {}
    virtual ~CurveFsClient() {}

    /**
     * @brief client 初始化
     *
     * @return 错误码
     */
    virtual int Init() = 0;

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
     * @param[out] seq 快照版本号
     *
     * @return 错误码
     */
    virtual int CreateSnapshot(const std::string &filename,
        uint64_t *seq) = 0;

    /**
     * @brief 删除快照
     *
     * @param filename 文件名
     * @param seq 快照版本号
     *
     * @return 错误码
     */
    virtual int DeleteSnapshot(
        const std::string &filename, uint64_t seq) = 0;

    /**
     * @brief 获取快照文件信息
     *
     * @param filename 文件名
     * @param seq 快照版本号
     * @param[out] snapInfo 快照文件信息
     *
     * @return 错误码
     */
    virtual int GetSnapshot(
        const std::string &filename, uint64_t seq, FInfo* snapInfo) = 0;

    /**
     * @brief 查询快照文件segment信息
     *
     * @param filename 文件名
     * @param seq 快照版本号
     * @param offset 偏移值
     * @param segInfo segment信息
     *
     * @return 错误码
     */
    virtual int GetSnapshotSegmentInfo(const std::string &filename,
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
     * @brief 删除snapshot chunk
     *
     * @param cidinfo chunk ID信息
     * @param seq 版本号
     *
     * @return 错误码
     */
    virtual int DeleteChunkSnapshot(ChunkIDInfo cidinfo,
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
};

class CurveFsClientImpl : public CurveFsClient {
 public:
    CurveFsClientImpl() {}
    virtual ~CurveFsClientImpl() {}

    // 以下接口定义见CurveFsClient接口注释
    int Init() override;

    int UnInit() override;

    int CreateSnapshot(const std::string &filename,
        uint64_t *seq) override;

    int DeleteSnapshot(const std::string &filename, uint64_t seq) override;

    int GetSnapshot(const std::string &filename,
        uint64_t seq,
        FInfo* snapInfo) override;

    int GetSnapshotSegmentInfo(const std::string &filename,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) override;

    int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf) override;

    int DeleteChunkSnapshot(ChunkIDInfo cidinfo,
        uint64_t seq) override;

    int GetChunkInfo(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo) override;

 private:
    ::curve::client::SnapshotClient client_;
};

}  // namespace snapshotserver
}  // namespace curve
#endif
