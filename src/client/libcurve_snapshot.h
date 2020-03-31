/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_LIBCURVE_SNAPSHOT_H_
#define SRC_CLIENT_LIBCURVE_SNAPSHOT_H_

#include <unistd.h>

#include <map>
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
  int Init(ClientConfigOption_t opt);

  /**
   * file对象初始化函数
   * @param: 配置文件路径
   */
  int Init(const std::string& configpath);

  /**
   * 创建快照
   * @param: userinfo是用户信息
   * @param: filename为要创建快照的文件名
   * @param: seq是出参，获取该文件的版本信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int CreateSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t* seq);
  /**
   * 删除快照
   * @param: userinfo是用户信息
   * @param: filename为要删除的文件名
   * @param: seq该文件的版本信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int DeleteSnapShot(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq);
  /**
   * 获取快照对应的文件信息
   * @param: userinfo是用户信息
   * @param: filename为对应的文件名
   * @param: seq为该文件打快照时对应的版本信息
   * @param: snapinfo是出参，保存当前文件的基础信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int GetSnapShot(const std::string& fname,
                             const UserInfo_t& userinfo,
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
  int ListSnapShot(const std::string& filename,
                            const UserInfo_t& userinfo,
                            const std::vector<uint64_t>* seqvec,
                            std::map<uint64_t, FInfo>* snapif);
  /**
   * 获取快照数据segment信息
   * @param: userinfo是用户信息
   * @param: filenam文件名
   * @param: seq是文件版本号信息
   * @param: offset是文件的偏移
   * @param：segInfo是出参，保存当前文件的快照segment信息
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int GetSnapshotSegmentInfo(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t seq,
                            uint64_t offset,
                            SegmentInfo *segInfo);

  /**
   * 读取seq版本号的快照数据
   * @param: cidinfo是当前chunk对应的id信息
   * @param: seq是快照版本号
   * @param: offset是快照内的offset
   * @param: len是要读取的长度
   * @param: buf是读取缓冲区
   * @param: scc是异步回调
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int ReadChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq, uint64_t offset,
                        uint64_t len, char *buf, SnapCloneClosure* scc);
  /**
   * 删除此次转储时产生的或者历史遗留的快照
   * 如果转储过程中没有产生快照，则修改chunk的correctedSn
   * @param: cidinfo是当前chunk对应的id信息
   * @param: correctedSeq是chunk需要修正的版本
   */
  int DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
                                                uint64_t correctedSeq);
  /**
   * 获取chunk的版本信息，chunkInfo是出参
   * @param: cidinfo是当前chunk对应的id信息
   * @param: chunkInfo是快照的详细信息
   */
  int GetChunkInfo(ChunkIDInfo cidinfo, ChunkInfoDetail *chunkInfo);
  /**
   * 获取快照状态
   * @param: userinfo是用户信息
   * @param: filenam文件名
   * @param: seq是文件版本号信息
   */
  int CheckSnapShotStatus(const std::string& filename,
                                const UserInfo_t& userinfo,
                                uint64_t seq,
                                FileStatus* filestatus);
  /**
   * @brief 创建clone文件
   * @detail
   *  - 若是clone，sn重置为初始值
   *  - 若是recover，sn不变
   *
   * @param source clone源文件名
   * @param: destination clone目标文件名
   * @param: userinfo 用户信息
   * @param: size 文件大小
   * @param: sn 版本号
   * @param: chunksize是要创建文件的chunk大小
   * @param[out] fileinfo 创建的目标文件的文件信息
   *
   * @return 错误码
   */
  int CreateCloneFile(const std::string& source, const std::string& destination,
                      const UserInfo_t& userinfo, uint64_t size, uint64_t sn,
                      uint32_t chunksize, FInfo* fileinfo);

  /**
   * @brief lazy 创建clone chunk
   * @param:location 数据源的url
   * @param:chunkidinfo 目标chunk
   * @param:sn chunk的序列号
   * @param:chunkSize chunk的大小
   * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
   * @param: scc是异步回调
   *
   * @return 错误码
   */
  int CreateCloneChunk(const std::string &location,
                       const ChunkIDInfo &chunkidinfo, uint64_t sn,
                       uint64_t correntSn, uint64_t chunkSize,
                       SnapCloneClosure* scc);

  /**
   * @brief 实际恢复chunk数据
   *
   * @param:chunkidinfo chunkidinfo
   * @param:offset 偏移
   * @param:len 长度
   * @param: scc是异步回调
   *
   * @return 错误码
   */
  int RecoverChunk(const ChunkIDInfo &chunkidinfo,
                   uint64_t offset, uint64_t len,
                   SnapCloneClosure* scc);

  /**
   * @brief 通知mds完成Clone Meta
   *
   * @param:destination 目标文件
   * @param:userinfo用户信息
   *
   * @return 错误码
   */
  int CompleteCloneMeta(const std::string &destination,
                                const UserInfo_t& userinfo);

  /**
   * @brief 通知mds完成Clone Chunk
   *
   * @param:destination 目标文件
   * @param:userinfo用户信息
   *
   * @return 错误码
   */
  int CompleteCloneFile(const std::string &destination,
                                const UserInfo_t& userinfo);

  /**
   * 设置clone文件状态
   * @param: filename 目标文件
   * @param: filestatus为要设置的目标状态
   * @param: userinfo用户信息
   * @param: fileId为文件ID信息，非必填
   *
   * @return 错误码
   */
  int SetCloneFileStatus(const std::string &filename,
                          const FileStatus& filestatus,
                          const UserInfo_t& userinfo,
                          uint64_t fileID = 0);

  /**
   * @brief 获取文件信息
   *
   * @param:filename 文件名
   * @param:userinfo 用户信息
   * @param[out] fileInfo 文件信息
   *
   * @return 错误码
   */
  int GetFileInfo(const std::string &filename,
                                const UserInfo_t& userinfo,
                                FInfo* fileInfo);

  /**
   * @brief 查询或分配文件segment信息
   *
   * @param:userinfo 用户信息
   * @param:offset 偏移值
   * @param:segInfo segment信息
   *
   * @return 错误码
   */
  int GetOrAllocateSegmentInfo(bool allocate,
                                uint64_t offset,
                                const FInfo_t* fi,
                                SegmentInfo *segInfo);

  /**
   * @brief 为recover rename复制的文件
   *
   * @param:userinfo 用户信息
   * @param:originId 被恢复的原始文件Id
   * @param:destinationId 克隆出的目标文件Id
   * @param:origin 被恢复的原始文件名
   * @param:destination 克隆出的目标文件
   *
   * @return 错误码
   */
  int RenameCloneFile(const UserInfo_t& userinfo,
                              uint64_t originId,
                              uint64_t destinationId,
                              const std::string &origin,
                              const std::string &destination);

  /**
   * 删除文件
   * @param: userinfo是用户信息
   * @param: filename待删除的文件名
   * @param: id为文件id，默认值为0，如果用户不指定该值，不会传id到mds
   */
  int DeleteFile(const std::string& filename,
                            const UserInfo_t& userinfo,
                            uint64_t id = 0);

  /**
   * 析构，回收资源
   */
  void UnInit();
  /**
   * 获取iomanager信息，测试代码使用
   */
  IOManager4Chunk* GetIOManager4Chunk() {return &iomanager4chunk_;}

 private:
  /**
   * 获取logicalpool中copyset的serverlist
   * @param: lpid是逻辑池id
   * @param: csid是逻辑池中的copysetid数据集
   * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
   */
  int GetServerList(const LogicPoolID& lpid,
                            const std::vector<CopysetID>& csid);

 private:
  // MDSClient负责与Metaserver通信，所有通信都走这个接口
  MDSClient               mdsclient_;

  // IOManager4Chunk用于管理发向chunkserver端的IO
  IOManager4Chunk         iomanager4chunk_;

  // 用于client 配置读取
  ClientConfig clientconfig_;
};
}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_LIBCURVE_SNAPSHOT_H_
