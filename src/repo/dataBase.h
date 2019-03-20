/*************************************************************************
> File Name: dataBase.h
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_REPO_DATABASE_H_
#define SRC_REPO_DATABASE_H_

#include <mysqlcurve/jdbc/mysql_connection.h>
#include <mysqlcurve/jdbc/cppconn/driver.h>
#include <mysqlcurve/jdbc/cppconn/exception.h>
#include <mysqlcurve/jdbc/cppconn/resultset.h>
#include <mysqlcurve/jdbc/cppconn/statement.h>
#include <mysqlcurve/jdbc/cppconn/prepared_statement.h>
#include <string>
#include <mutex> //NOLINT

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

  DataBase(const std::string &url,
           const std::string &user,
           const std::string &password);

  ~DataBase();

  int connectDB();

  // CRUD
  int Exec(const std::string &sql);

  int ExecUpdate(const std::string &sql);

  int QueryRows(const std::string &sql, sql::ResultSet **res);

 private:
  bool CheckConn();

 public:
  std::string url_;
  std::string user_;
  std::string password_;

  sql::Connection *conn_;
  sql::Statement *statement_;

  std::mutex mutex_;
};
}  // namespace repo
}  // namespace curve

#endif  // SRC_REPO_DATABASE_H_
