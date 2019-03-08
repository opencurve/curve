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

#include "src/mds/nameserver2/etcd_client.h"
#include "src/common/encode.h"
#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {

enum class StoreStatus {
    OK = 0,
    KeyNotExist,
    InternalError,
};
std::ostream& operator << (std::ostream & os, StoreStatus &s);

const char FILEINFOKEYPREFIX[] = "01";
const char SEGMENTINFOKEYPREFIX[] = "02";
const char SNAPSHOTFILEINFOKEYPREFIX[] = "03";
// TODO(hzsunjianliang): if use single prefix for snapshot file?
const int PREFIX_LENGTH = 2;
const int SEGMENTKEYLEN = 18;

std::string EncodeFileStoreKey(uint64_t parentID, const std::string &fileName);
std::string EncodeSnapShotFileStoreKey(uint64_t parentID,
                const std::string &fileName);
std::string EncodeSegmentStoreKey(uint64_t inodeID, offset_t offset);

bool EncodeFileInfo(const FileInfo &finlInfo, std::string *out);

bool DecodeFileInfo(const std::string info, FileInfo *fileInfo);

bool EncodeSegment(const PageFileSegment &segment, std::string *out);

bool DecodeSegment(const std::string info, PageFileSegment *segment);

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
   * @param[in] storeKey fileInfo对应的编码后的key
   * @param[in] fileInfo 文件元信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus PutFile(const std::string & storeKey,
                const FileInfo & fileInfo) = 0;

  /**
   * @brief GetFile 获取指定file的元数据信息
   *
   * @param[in] storeKey 
   * @param[out] 从storage中获得的元数据信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus GetFile(const std::string & storeKey,
                FileInfo * fileInfo) = 0;

  /**
   * @brief DeleteFile 删除指定文件元数据信息
   *
   * @param[in] storeKey
   *
   * @retuen StoreStatus 错误码
   */
  virtual StoreStatus DeleteFile(const std::string & storeKey) = 0;

  /**
   * @brief RenameFile 事务，存储新的file的元数据信息，删除旧的元数据信息
   *
   * @param[in] oldStoreKey 
   * @param[in] oldFileInfo
   * @param[in] newStoreKey
   * @param[in] newFileInfo
   *
   * @return StoreStaus 错误码
   */
  virtual StoreStatus RenameFile(const std::string & oldStoreKey,
                                   const FileInfo &oldfileInfo,
                                   const std::string & newStoreKey,
                                   const FileInfo &newfileInfo) = 0;
  /**
   * @brief ListFile 获取[startKey, endKey)之间的所有文件
   *
   * @param[in] startStoreKey
   * @param[in] endStoreKey
   * @param[out] files 所有文件列表
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus ListFile(const std::string & startStoreKey,
                                 const std::string & endStoreKey,
                                 std::vector<FileInfo> * files) = 0;

  /**
   * @brief GetSegment 获取指定segment信息
   *
   * @param[in] StoreKey
   * @param[out] segment segment信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus GetSegment(const std::string & storeKey,
                                   PageFileSegment *segment) = 0;

  /**
   * @brief PutSegment 存储指定的segment信息
   *
   * @param[in] storeKey
   * @param[in] segment 元数据信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus PutSegment(const std::string & storeKey,
                                   const PageFileSegment * segment) = 0;

  /**
   * @brief DeleteSegment 删除指定的segemt元数据
   *
   * @param[in] storeKey
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus DeleteSegment(const std::string &storeKey) = 0;

  /**
   * @brief SnapShotFile 事务，存储snapshotFile的元数据信息，更新源文件元数据
   *
   * @param[in] originalFileKey
   * @param[in] originalFileInfo 打快照的源文件元信息
   * @param[in] snapshotFileKey
   * @param[in] snapshotFileInfo 快照文件元信息
   *
   * @return StoreStatus 错误码
   */
  virtual StoreStatus SnapShotFile(const std::string & originalFileKey,
                                    const FileInfo *originalFileInfo,
                                    const std::string & snapshotFileKey,
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

  StoreStatus PutFile(const std::string & storeKey,
                const FileInfo & fileInfo) override;

  StoreStatus GetFile(const std::string & storeKey,
                FileInfo * fileInfo) override;

  StoreStatus DeleteFile(const std::string & storeKey) override;

  StoreStatus RenameFile(const std::string & oldStoreKey,
                            const FileInfo &oldfileInfo,
                            const std::string & newStoreKey,
                            const FileInfo &newfileInfo) override;

  StoreStatus ListFile(const std::string & startStoreKey,
                            const std::string & endStoreKey,
                            std::vector<FileInfo> * files) override;

  StoreStatus GetSegment(const std::string & storeKey,
                            PageFileSegment *segment) override;

  StoreStatus PutSegment(const std::string & storeKey,
                            const PageFileSegment * segment) override;

  StoreStatus DeleteSegment(const std::string &storeKey) override;

  StoreStatus SnapShotFile(const std::string & originalFileKey,
                                const FileInfo *originalFileInfo,
                                const std::string & snapshotFileKey,
                                const FileInfo * snapshotFileInfo) override;

  StoreStatus LoadSnapShotFile(std::vector<FileInfo> *snapShotFiles) override;

 private:
    StoreStatus getErrorCode(int errCode);

 private:
  std::shared_ptr<StorageClient> client_;
};
}  // namespace mds
}  // namespace curve


#endif   // SRC_MDS_NAMESERVER2_NAMESPACE_STORAGE_H_
