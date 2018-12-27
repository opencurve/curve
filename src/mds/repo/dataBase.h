/*
 * Project: curve
 * Created Date: Thu Sep 06 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef REPO_DATABASE_H_
#define REPO_DATABASE_H_

#include <mysqlcurve/jdbc/mysql_connection.h>
#include <mysqlcurve/jdbc/cppconn/driver.h>
#include <mysqlcurve/jdbc/cppconn/exception.h>
#include <mysqlcurve/jdbc/cppconn/resultset.h>
#include <mysqlcurve/jdbc/cppconn/statement.h>
#include <mysqlcurve/jdbc/cppconn/prepared_statement.h>
#include <string>

namespace curve {
namespace repo {
const int OperationOK = 0;
const int SqlException = -1;
const int RuntimeExecption = -2;

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

 public:
  std::string url_;
  std::string user_;
  std::string password_;

  sql::Connection *conn_;
  sql::Statement *statement_;
};
}  // namespace repo
}  // namespace curve

#endif  // REPO_DATABASE_H_
