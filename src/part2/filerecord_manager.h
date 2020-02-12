/*
 * Project: nebd
 * Created Date: Thursday February 13th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_FILERECORD_MANAGER_H_
#define SRC_PART2_FILERECORD_MANAGER_H_

#include <limits.h>
#include <unordered_map>
#include <memory>
#include <string>

#include "src/common/rw_lock.h"
#include "src/part2/define.h"
#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {

using nebd::common::RWLock;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;
using FileRecordMap = std::unordered_map<int, NebdFileRecord>;

// nebd server 文件记录管理（包括内存状态和持久化状态）
class FileRecordManager {
 public:
    explicit FileRecordManager(MetaFileManagerPtr metaFileManager)
        : metaFileManager_(metaFileManager) {}
    virtual ~FileRecordManager() {}
    /**
     * 从持久化文件中加载文件记录到内存
     * @return: 成功返回0，失败返回-1
     */
    virtual int Load();
    /**
     * 根据fd获取指定文件的内存记录
     * @param fd: 文件的fd
     * @param fileRecord[out]: 文件记录
     * @return: 成功返回true，失败返回false
     */
    virtual bool GetRecord(int fd, NebdFileRecord* fileRecord);
    /**
     * 根据文件名获取指定文件的内存记录
     * @param fileName: 文件名称
     * @param fileRecord[out]: 文件记录
     * @return: 成功返回true，失败返回false
     */
    virtual bool GetRecord(const std::string& fileName,
                           NebdFileRecord* fileRecord);
    /**
     * 修改文件持久化内容，并更新文件内存记录
     * 如果记录不存在，插入一条新的记录
     * 如果相同文件名的记录已存在，但是fd不同，则删除旧的记录，再插入当前记录
     * 如果相同fd的记录已存在，但是文件名不同，则返回失败
     * @param fileRecord: 文件记录
     * @return: 成功返回true，失败返回false
     */
    virtual bool UpdateRecord(const NebdFileRecord& fileRecord);
    /**
     * 删除指定fd对应的文件记录，先更新元数据文件，然后跟新内存记录
     * @param fd: 文件的fd
     * @return: 成功返回true，失败返回false
     */
    virtual bool RemoveRecord(int fd);
    /**
     * 判断指定fd对应的文件记录是否存在
     * @param fd: 文件的fd
     * @return: 如果存在返回true，不存在返回false
     */
    virtual bool Exist(int fd);
    /**
     * 清楚所有的记录
     */
    virtual void Clear();
    /**
     * 获取所有文件记录的映射表
     * @return: 返回所有文件记录的映射表
     */
    virtual FileRecordMap ListRecords();
    /**
     * 更新文件内存记录的时间戳
     * @param fd: 指定的文件fd
     * @param timestamp: 要变更为的时间戳值
     * @return: 成功返回true，指定fd不存在返回false
     */
    virtual bool UpdateFileTimestamp(int fd, uint64_t timestamp);
    /**
     * 获取文件内存记录的时间戳
     * @param fd: 指定的文件fd
     * @param timestamp[out]: 获取到的时间戳值
     * @return: 成功返回true，指定fd不存在返回false
     */
    virtual bool GetFileTimestamp(int fd, uint64_t* timestamp);
    /**
     * 更新文件内存记录的状态
     * @param fd: 指定的文件fd
     * @param status: 要变更为的文件状态
     * @return: 成功返回true，指定fd不存在返回false
     */
    virtual bool UpdateFileStatus(int fd, NebdFileStatus status);
    /**
     * 获取文件内存记录的状态
     * @param fd: 指定的文件fd
     * @param status[out]: 获取到的文件状态
     * @return: 成功返回true，指定fd不存在返回false
     */
    virtual bool GetFileStatus(int fd, NebdFileStatus* status);

 private:
    // 获取指定文件名的文件记录（非线程安全）
    int FindRecordUnlocked(const std::string& fileName);

 private:
    // 保护文件记录映射表的读写锁
    RWLock rwLock_;
    // 文件记录映射表
    FileRecordMap map_;
    // 持久化元数据文件管理
    MetaFileManagerPtr metaFileManager_;
};
using FileRecordManagerPtr = std::shared_ptr<FileRecordManager>;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILERECORD_MANAGER_H_
