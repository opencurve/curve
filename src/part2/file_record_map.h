/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_FILE_RECORD_MAP_H_
#define SRC_PART2_FILE_RECORD_MAP_H_

#include <brpc/closure_guard.h>
#include <limits.h>
#include <unordered_map>
#include <memory>
#include <string>

#include "src/common/rw_lock.h"
#include "src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::RWLock;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;

// nebd server 文件信息的内存记录
class FileRecordMap {
 public:
    FileRecordMap() {}
    virtual ~FileRecordMap() {}
    /**
     * 根据fd获取指定文件的内存记录
     * @param fd: 文件的fd
     * @return: 文件的内存记录
     */
    NebdFileRecordPtr GetRecord(int fd);
    /**
     * 根据文件名获取指定文件的内存记录
     * @param fileName: 文件名称
     * @return: 文件的内存记录
     */
    NebdFileRecordPtr GetRecord(const std::string& fileName);
    /**
     * 更新文件内存记录
     * 如果记录不存在，插入一条新的记录
     * 如果相同文件名的记录已存在，但是fd不同，则删除旧的记录，再插入当前记录
     * 如果相同fd的记录已存在，但是文件名不同，则返回失败
     * @param fileRecord: 文件记录
     * @return: 成功返回true，失败返回false
     */
    bool UpdateRecord(NebdFileRecordPtr fileRecord);
    /**
     * 根据指定fd对应的文件记录
     * @param fd: 文件的fd
     */
    void RemoveRecord(int fd);
    /**
     * 判断指定fd对应的文件记录是否存在
     * @param fd: 文件的fd
     * @return: 如果存在返回true，不存在返回false
     */
    bool Exist(int fd);
    /**
     * 清楚所有的记录
     */
    void Clear();
    /**
     * 获取所有文件记录的映射表
     * @return: 返回所有文件记录的映射表
     */
    std::unordered_map<int, NebdFileRecordPtr> ListRecords();

 private:
    // 获取指定文件名的文件记录（非线程安全）
    int FindRecordUnlocked(const std::string& fileName);

 private:
    // 保护文件记录映射表的读写锁
    RWLock rwLock_;
    // 文件记录映射表
    std::unordered_map<int, NebdFileRecordPtr> map_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_RECORD_MAP_H_
