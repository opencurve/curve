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

#include "src/client2/client_common.h"
#include "src/client2/libcurve_chunk.h"

using ::curve::client::SegmentInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::CopysetID;
using ::curve::client::ChunkID;
using ::curve::client::ChunkInfoDetail;
using ::curve::client::ChunkIDInfo;

namespace curve {
namespace snapshotserver {

class CurveFsClient {
 public:
    CurveFsClient() {}
    virtual ~CurveFsClient() {}
    // client 初始化
    virtual int Init() = 0;
    // client 资源回收
    virtual int UnInit() = 0;
    // 创建快照
    // 输入 ： 目标文件名
    // 输出 ：  seqNum, 错误码
    virtual int CreateSnapshot(const std::string &filename,
        uint64_t *seq) = 0;
    // 删除快照
    // 输入 ： 目标文件 + 快照版本号
    // 输出 ： OK or 错误码
    virtual int DeleteSnapshot(
        const std::string &filename, uint64_t seq) = 0;

    virtual int GetSnapshot(
        const std::string &filename, uint64_t seq, FInfo* snapInfo) = 0;

    // 查询快照文件segment信息
    // 输入 ： 目标文件名 + 版本号 + offset
    // 输出 ： OK or 错误码 + segment信息
    virtual int GetSnapshotSegmentInfo(const std::string &filename,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) = 0;
    // 读取snapshot chunk的数据
    virtual int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf) = 0;
    // 删除snapshot chunk
    virtual int DeleteChunkSnapshot(ChunkIDInfo cidinfo,
        uint64_t seq) = 0;
    // 获取chunk的版本号信息
    virtual int GetChunkInfo(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo) = 0;
};

class CurveFsClientImpl : public CurveFsClient {
 public:
    CurveFsClientImpl() {}
    virtual ~CurveFsClientImpl() {}
    // client 初始化
    int Init() override;
    // client 资源回收
    int UnInit() override;
    // 创建快照
    // 输入 ： 目标文件名
    // 输出 ：  seqNum, 错误码
    int CreateSnapshot(const std::string &filename,
        uint64_t *seq) override;
    // 删除快照
    // 输入 ： 目标文件 + 快照版本号
    // 输出 ： OK or 错误码
    int DeleteSnapshot(const std::string &filename, uint64_t seq) override;

    int GetSnapshot(const std::string &filename,
        uint64_t seq,
        FInfo* snapInfo) override;

    // 查询快照文件segment信息
    // 输入 ： 目标文件名 + 版本号 + offset
    // 输出 ： OK or 错误码 + segment信息
    int GetSnapshotSegmentInfo(const std::string &filename,
        uint64_t seq,
        uint64_t offset,
        SegmentInfo *segInfo) override;
    // 读取snapshot chunk的数据
    int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf) override;
    // 删除snapshot chunk
    int DeleteChunkSnapshot(ChunkIDInfo cidinfo,
        uint64_t seq) override;
    // 获取chunk的版本号信息
    int GetChunkInfo(ChunkIDInfo cidinfo,
        ChunkInfoDetail *chunkInfo) override;

 private:
    ::curve::client::SnapshotClient client_;
};

}  // namespace snapshotserver
}  // namespace curve
#endif
