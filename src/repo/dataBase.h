/*************************************************************************
> File Name: dataBase.h
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_REPO_DATABASE_H_
#define SRC_REPO_DATABASE_H_

#include <string>
#include <mutex> //NOLINT

#include "src/repo/connPool.h"

namespace curve {
namespace repo {
const int OperationOK = 0;
const int SqlException = -1;
const int RuntimeExecption = -2;
const int ConnLost = -3;
const int InternalError = -4;
// 数据库基础操作类
class DataBase {
 public:
  DataBase() = default;

  DataBase(const std::string &user,
           const std::string &url,
           const std::string &password,
           const std::string &schema,
           uint32_t capacity);

  virtual ~DataBase();
  virtual int InitDB();

  // CRUD
  // 执行数据库的创建删除操作
  virtual int Execute(const std::string &sql);
  // 在数据库schema已经存在的情况下执行sql语句
  virtual int ExecUpdate(const std::string &sql);
  // 执行数据库查询语句
  virtual int QueryRows(const std::string &sql, sql::ResultSet **res);

 private:
  // 数据库连接池对象
  ConnPool* connPool_;
  // 数据库服务地址
  std::string url_;
  // 数据库用户名
  std::string user_;
  // 数据库认证密码
  std::string password_;
  // 数据库连接池最大连接上限
  uint32_t connPoolCapacity_;
  // 数据库schema
  std::string schema_;
};
}  // namespace repo
}  // namespace curve

#endif  // SRC_REPO_DATABASE_H_
