/*
 * Project: curve
 * Created Date: Wed Sep 04 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef SRC_REPO_REPO_H_
#define SRC_REPO_REPO_H_

#include <string>
#include <map>
#include <list>
#include <vector>

#include "src/repo/dataBase.h"
#include "src/repo/sqlStatement.h"
namespace curve {
namespace repo {

/**
 * @brief repo的接口类，提供repo的公共接口
 * @detail
 * -连接数据库
 * -创建需要的表文件
 * -创建/删除和use数据库
 */
class RepoInterface {
 public:
  /**
   * @brief 创建数据库实例并发起连接
   * @param 数据库名称
   * @param 用户名
   * @param 数据库访问地址
   * @param 密码
   * @return 错误码
   *
   */

  virtual int connectDB(const std::string &dbName,
                        const std::string &user,
                        const std::string &url,
                        const std::string &password) = 0;

  /**
   * @brief 创建repo需要的表文件
   * @return 错误码
   */
  virtual int createAllTables() = 0;
  /**
   * @brief 创建数据库
   * @return 错误码
   */
  virtual int createDatabase() = 0;
  /**
   * @brief 切换数据库
   * @return 错误码
   */
  virtual int useDataBase() = 0;
  /**
   * @brief 删除数据库
   * @return 错误码
   */
  virtual int dropDataBase() = 0;
};
/**
 * @brief repo中item项接口
 */
struct RepoItem {
 public:
  /**
   * @brief 获取数据记录信息
   * @param[out] 保存数据信息的map
   */
  virtual void getKV(std::map<std::string, std::string> *kv) const = 0;
  /**
   * @brief 获取主键信息
   * @param[out] 保存主键信息的map
   */
  virtual void getPrimaryKV(
      std::map<std::string,
               std::string> *primary) const = 0;
  /**
   * @brief 获取记录项归属的表
   * @return 表名
   */
  virtual std::string getTable() const = 0;
};

/**
 * @brief sql语句拼接类
 * @detail 拼接CURD sql语句
 */
static class MakeSql {
 public:
  /**
   * @brief 拼接insert语句
   * @param 数据项
   * @return insert sql语句
   */
  std::string makeInsert(const RepoItem &t);
  /**
   * @brief 拼接查询语句
   * @param 数据项
   * @return select sql语句
   */
  std::string makeQueryRows(const RepoItem &t);

  /**
   * @brief 拼接查询语句
   * @param 数据项
   * @return select sql语句
   */
  std::string makeQueryRow(const RepoItem &t);

  /**
   * @brief 拼接删除语句
   * @param 数据项
   * @return delete sql语句
   */
  std::string makeDelete(const RepoItem &t);

  /**
   * @brief 拼接更新语句
   * @param 数据项
   * @return update sql语句
   */
  std::string makeUpdate(const RepoItem &t);

 private:
  /**
   * @brief 生成条件字符串
   * @param 数据项
   * @return 条件语句字符串
   */
  std::string makeCondtion(const RepoItem &t);
} makeSql;

inline std::string convertToSqlValue(const std::string value) {
      return "'" + value + "'";
}

}  // namespace repo
}  // namespace curve

#endif  // SRC_REPO_REPO_H_
