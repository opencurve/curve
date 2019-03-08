/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_LIBCURVE_CHUNK_H
#define CURVE_LIBCURVE_CHUNK_H

#include <unistd.h>
#include <string>
#include <atomic>
#include <vector>
#include <unordered_map>

#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "src/client/iomanager4chunk.h"

namespace curve {
namespace client {
// SnapshotClient为外围快照系统与MDS和Chunkserver通信的出口
class SnapshotClient {
 public:
  SnapshotClient();
  ~SnapshotClient() = default;
  /**
   * 初始化函数，外围系统直接传入配置选项
   * @param: opt为外围配置选项
   * @return：0为成功，-1为失败
   */
  LIBCURVE_ERROR Init(ClientConfigOption_t opt);
  /**
   * 创建快照
   * @param: userinfo是用户信息
   * @param: filename为要创建快照的文件名
   * @param: seq是出参，获取该文件的版本信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR CreateSnapShot(std::string filename,
                                UserInfo_t userinfo,
                                uint64_t* seq);
  /**
   * 删除快照
   * @param: userinfo是用户信息
   * @param: filename为要删除的文件名
   * @param: seq该文件的版本信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR DeleteSnapShot(std::string filename,
                                UserInfo_t userinfo,
                                uint64_t seq);
  /**
   * 获取快照对应的文件信息
   * @param: userinfo是用户信息
   * @param: filename为对应的文件名
   * @param: seq为该文件打快照时对应的版本信息
   * @param: snapinfo是出参，保存当前文件的基础信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR GetSnapShot(std::string fname,
                             UserInfo_t userinfo,
                             uint64_t seq,
                             FInfo* snapinfo);
  /**
   * 列出当前文件对应版本列表的文件信息
   * @param: userinfo是用户信息
   * @param: filenam文件名
   * @param: seqvec是当前文件的版本列表
   * @param: snapif是出参，获取多个seq号的文件信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR ListSnapShot(std::string filename,
                            UserInfo_t userinfo,
                            const std::vector<uint64_t>* seqvec,
                            std::vector<FInfo*>* snapif);
  /**
   * 获取快照数据segment信息
   * @param: userinfo是用户信息
   * @param: filenam文件名
   * @param: seq是文件版本号信息
   * @param: offset是文件的偏移
   * @param：segInfo是出参，保存当前文件的快照segment信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR GetSnapshotSegmentInfo(std::string filename,
                            UserInfo_t userinfo,
                            LogicalPoolCopysetIDInfo* lpcsIDInfo,
                            uint64_t seq,
                            uint64_t offset,
                            SegmentInfo *segInfo);
  /**
   * 获取logicalpool中copyset的serverlist
   * @param: lpid是逻辑池id
   * @param: csid是逻辑池中的copysetid数据集
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR GetServerList(const LogicPoolID& lpid,
                            const std::vector<CopysetID>& csid);
  /**
   * 读取seq版本号的快照数据
   * @param: cidinfo是当前chunk对应的id信息
   * @param: seq是快照版本号
   * @param: offset是快照内的offset
   * @param: len是要读取的长度
   * @param: buf是读取缓冲区
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  LIBCURVE_ERROR ReadChunkSnapshot(ChunkIDInfo cidinfo,
                          uint64_t seq,
                          uint64_t offset,
                          uint64_t len,
                          void *buf);
  /**
   * 删除seq版本号的快照数据
   * @param: cidinfo是当前chunk对应的id信息
   * @param: seq是快照版本号
   */
  LIBCURVE_ERROR DeleteChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq);
  /**
   * 获取chunk的版本信息，chunkInfo是出参
   * @param: cidinfo是当前chunk对应的id信息
   * @param: chunkInfo是快照的详细信息
   */
  LIBCURVE_ERROR GetChunkInfo(ChunkIDInfo cidinfo, ChunkInfoDetail *chunkInfo);
  /**
   * 获取快照状态
   * @param: userinfo是用户信息
   * @param: filenam文件名
   * @param: seq是文件版本号信息
   */
  LIBCURVE_ERROR CheckSnapShotStatus(std::string filename,
                                UserInfo_t userinfo,
                                uint64_t seq);
  /**
   * 析构，回收资源
   */
  void UnInit();
  /**
   * 获取iomanager信息，测试代码使用
   */
  IOManager4Chunk* GetIOManager4Chunk() {return &iomanager4chunk_;}

 private:
  // MDSClient负责与Metaserver通信，所有通信都走这个接口
  MDSClient               mdsclient_;

  // IOManager4Chunk用于管理发向chunkserver端的IO
  IOManager4Chunk         iomanager4chunk_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_H
