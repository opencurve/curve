/*************************************************************************
> File Name: snapshotcloneRepo.h
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_DAO_SNAPSHOTCLONEREPO_H_
#define SRC_SNAPSHOTCLONESERVER_DAO_SNAPSHOTCLONEREPO_H_

#include <list>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "src/repo/repo.h"

using namespace ::curve::repo; //NOLINT

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 快照记录数据项接口类
 *
 */
struct SnapshotRepoItem : public curve::repo::RepoItem {
 public:
  std::string uuid;
  std::string user;
  std::string fileName;
  std::string desc;
  uint64_t seqNum;
  uint32_t chunkSize;
  uint64_t segmentSize;
  uint64_t fileLength;
  uint64_t time;
  int status;
  SnapshotRepoItem() = default;
  explicit SnapshotRepoItem(const std::string &uuid);
  SnapshotRepoItem(const std::string &uuid,
                const std::string &user,
                const std::string &filename,
                const std::string &desc,
                uint64_t seq,
                uint32_t csize,
                uint64_t segsize,
                uint64_t flen,
                uint64_t time,
                int status);

  bool operator==(const SnapshotRepoItem &r);

  // 参见虚基类注释说明
  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

/**
 * @brief 克隆记录数据项接口类
 *
 */
struct CloneRepoItem : public curve::repo::RepoItem {
 public:
  std::string taskID;
  std::string user;
  uint8_t tasktype;
  std::string src;
  std::string dest;
  uint64_t originID;
  uint64_t destID;
  uint64_t time;
  uint8_t filetype;
  bool isLazy;
  uint8_t nextstep;
  uint8_t status;
  CloneRepoItem() = default;
  explicit CloneRepoItem(const std::string &taskID);
  CloneRepoItem(const std::string &taskID,
                const std::string &user,
                uint8_t tasktype,
                const std::string &src,
                const std::string &dest,
                uint64_t originID,
                uint64_t destID,
                uint64_t time,
                uint8_t filetype,
                bool isLazy,
                uint8_t nextstep,
                uint8_t status);

  bool operator==(const CloneRepoItem &r);

  // 参见虚基类注释说明
  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

class SnapshotCloneRepo : public curve::repo::RepoInterface {
 public:
  SnapshotCloneRepo() {}

  ~SnapshotCloneRepo() {}
  // 参见虚基类注释说明
  // TODO(hzzhaojianming) 可以考虑将入参整合成一个option
  int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password,
                uint32_t poolSize) override;

  int createAllTables() override;

  int createDatabase() override;

  int useDataBase() override;

  int dropDataBase() override;

  /**
   * @brief 获取database实例
   * @return database实例的智能指针
   */
  std::shared_ptr<curve::repo::DataBase> getDataBase();

  void setDataBase(std::shared_ptr<curve::repo::DataBase> db);


  /**
   * @brief 插入一条快照记录
   * @param 快照记录对象
   * @return 错误码
   *  操作ok 0
   *  sql执行异常 -1
   *  runtime异常 -2
   *  连接断开 -3
   */
  virtual int InsertSnapshotRepoItem(const SnapshotRepoItem &sr);
  /**
   * @brief 删除一条快照记录
   * @param 快照uuid
   * @return 错误码（同上）
   */
  virtual int DeleteSnapshotRepoItem(const std::string &uuid);
  /**
   * @brief 更新一条快照记录
   * @param 快照记录对象
   * @return 错误码（同上）
   */
  virtual int UpdateSnapshotRepoItem(const SnapshotRepoItem &sr);
  /**
   * @brief 查询一条快照记录
   * @param 快照uuid
   * @param[out] 快照记录对象指针
   * @return 错误码（同上）
   */
  virtual int QuerySnapshotRepoItem(const std::string &uuid,
                                    SnapshotRepoItem *sr);
  /**
   * @brief 加载快照记录
   * @param 快照记录对象vector指针
   * @return 错误码（同上）
   */
  virtual int LoadSnapshotRepoItems(
              std::vector<SnapshotRepoItem> *snapshotlist);
  /**
   * @breif 插入一条克隆记录
   * @param 克隆记录
   * @return 错误码（同上）
   */
  virtual int InsertCloneRepoItem(const CloneRepoItem &cr);
  /**
   * @brief 删除一条克隆记录
   * @param 克隆任务ID
   * @return 错误码（同上）
   */
  virtual int DeleteCloneRepoItem(const std::string &taskID);
  /**
   * @brief 更新一条克隆记录
   * @param 克隆记录对象
   * @return 错误码（同上）
   */
  virtual int UpdateCloneRepoItem(const CloneRepoItem &cr);
  /**
   * @brief 查询一条克隆记录
   * @param 克隆任务ID
   * @param[out] 克隆记录对象指针
   * @return 错误码（同上）
   */
  virtual int QueryCloneRepoItem(const std::string &taskID,
                                    CloneRepoItem *cr);
  /**
   * @brief 加载克隆记录
   * @param 克隆记录对象vector指针
   * @return 错误码（同上）
   */
  virtual int LoadCloneRepoItems(
              std::vector<CloneRepoItem> *clonelist);


 private:
  // database指针
  std::shared_ptr<curve::repo::DataBase> db_;
  // 数据库名称
  std::string dbName_;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_DAO_SNAPSHOTCLONEREPO_H_
