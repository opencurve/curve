/*
 * Project: curve
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#ifndef SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_
#define SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_

#include <string>
#include <tuple>
#include <vector>
#include <iostream>
#include <map>
#include "proto/nameserver2.pb.h"

#include "src/common/encode.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/etcd_client.h"
#include "src/mds/nameserver2/namespace_helper.h"

namespace curve {
namespace mds {

enum class StoreStatus {
    OK = 0,
    KeyNotExist,
    InternalError,
};
std::ostream& operator << (std::ostream & os, StoreStatus &s);

// TODO(hzsunjianliang): may be storage need high level abstruction
// put the encoding internal, not external


// TODO(lixiaocui): 接口不传入key，直接在函数中encode, 只需要依赖info就可以
// kv value storage for namespace and segment
class NameServerStorage {
 public:
  virtual ~NameServerStorage(void) {}

  /**
   * @brief PutFile 存储fileInfo信息
   *
   * @param[in] fileInfo 文件元信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus PutFile(const FileInfo & fileInfo) = 0;

  /**
   * @brief GetFile 获取指定file的元数据信息
   *
   * @param[in] id 需要获取文件信息的文件parent inode id
   * @param[in] filename需要获取文件信息的文件名
   * @param[out] 从storage中获得的元数据信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus GetFile(InodeID id,
                              const std::string &filename,
                              FileInfo * fileInfo) = 0;

  /**
   * @brief DeleteFile 删除文件
   *
   * @param[in] id 需要获取文件信息的文件parent inode id
   * @param[in] filename需要获取文件信息的文件名
   *
   * @retuen StoreStatus 错误码
   */
  virtual StoreStatus DeleteFile(InodeID id,
                              const std::string &filename) = 0;

  /**
   * @brief DeleteSnapshotFile 删除快照文件
   *
   * @param[in] id 需要获取文件信息的文件parent inode id
   * @param[in] filename需要获取文件信息的文件名
   *
   * @retuen StoreStatus 错误码
   */
  virtual StoreStatus DeleteSnapshotFile(InodeID id,
                              const std::string &filename) = 0;

  /**
   * @brief RenameFile 事务，存储新的file的元数据信息，删除旧的元数据信息
   *
   * @param[in] oldFileInfo
   * @param[in] newFileInfo
   *
   * @return StoreStaus 错误码
   */
  virtual StoreStatus RenameFile(const FileInfo &oldfileInfo,
                                 const FileInfo &newfileInfo) = 0;
  /**
   * @brief ListFile 获取[startid, endid)之间的所有文件
   *
   * @param[in] startidid为起始id
   * @param[in] endid为结束id
   * @param[out] files 所有文件列表
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus ListFile(InodeID startid,
                               InodeID endid,
                               std::vector<FileInfo> * files) = 0;

  /**
   * @brief ListSnapshotFile 获取[startid, endid)之间的所有快照文件
   *
   * @param[in] startidid为起始id
   * @param[in] endid为结束id
   * @param[out] files 所有文件列表
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus ListSnapshotFile(InodeID startid,
                               InodeID endid,
                               std::vector<FileInfo> * files) = 0;

  /**
   * @brief GetSegment 获取指定segment信息
   *
   * @param[in] id为当前文件的inode
   * @param[in] off为当前segment的偏移
   * @param[out] segment segment信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus GetSegment(InodeID id,
                                 uint64_t off,
                                 PageFileSegment *segment) = 0;

  /**
   * @brief PutSegment 存储指定的segment信息
   *
   * @param[in] id为当前文件的inode
   * @param[in] off为当前segment的偏移
   * @param[out] segment segment信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus PutSegment(InodeID id,
                                 uint64_t off,
                                 const PageFileSegment * segment) = 0;

  /**
   * @brief DeleteSegment 删除指定的segment元数据
   *
   * @param[in] id为当前文件的inode
   * @param[in] off为当前segment的偏移
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus DeleteSegment(InodeID id, uint64_t off) = 0;

  /**
   * @brief SnapShotFile 事务，存储snapshotFile的元数据信息，更新源文件元数据
   *
   * @param[in] originalFileInfo 打快照的源文件元信息
   * @param[in] snapshotFileInfo 快照文件元信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus SnapShotFile(const FileInfo *originalFileInfo,
                                   const FileInfo *snapshotFileInfo) = 0;

  /**
   * @brief LoadSnapShotFile 加载所有snapshotFile元信息
   *
   * @param[out] snapshotFiles 快照元信息列表
   *
   * @retrun StoreStatus 错误码
   */
  virtual StoreStatus LoadSnapShotFile(
                                std::vector<FileInfo> *snapShotFiles) = 0;
};

class NameServerStorageImp : public NameServerStorage {
 public:
  explicit NameServerStorageImp(std::shared_ptr<StorageClient> client);
  ~NameServerStorageImp() {}

  StoreStatus PutFile(const FileInfo & fileInfo) override;

  StoreStatus GetFile(InodeID id,
                      const std::string &filename,
                      FileInfo * fileInfo) override;

  StoreStatus DeleteFile(InodeID id,
                         const std::string &filename) override;

  StoreStatus DeleteSnapshotFile(InodeID id,
                         const std::string &filename) override;

  StoreStatus RenameFile(const FileInfo &oldfileInfo,
                         const FileInfo &newfileInfo) override;

  StoreStatus ListFile(InodeID startid,
                       InodeID endid,
                       std::vector<FileInfo> * files) override;

  StoreStatus ListSnapshotFile(InodeID startid,
                       InodeID endid,
                       std::vector<FileInfo> * files) override;

  StoreStatus GetSegment(InodeID id,
                         uint64_t off,
                         PageFileSegment *segment) override;

  StoreStatus PutSegment(InodeID id,
                         uint64_t off,
                         const PageFileSegment * segment) override;

  StoreStatus DeleteSegment(InodeID id, uint64_t off) override;

  StoreStatus SnapShotFile(const FileInfo *originalFileInfo,
                           const FileInfo * snapshotFileInfo) override;

  StoreStatus LoadSnapShotFile(std::vector<FileInfo> *snapShotFiles) override;

 private:
    StoreStatus ListFileInternal(const std::string& startStoreKey,
                                 const std::string& endStoreKey,
                                 std::vector<FileInfo> *files);
    StoreStatus GetStoreKey(FileType filetype,
                            InodeID id,
                            const std::string& filename,
                            std::string* storekey);
    StoreStatus getErrorCode(int errCode);

 private:
  std::shared_ptr<StorageClient> client_;
};
}  // namespace mds
}  // namespace curve


#endif   // SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_
