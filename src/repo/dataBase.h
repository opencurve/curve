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
// 操作成功
const int OperationOK = 0;
// sql执行异常
const int SqlException = -1;
// mysql runtime异常
const int RuntimeExecption = -2;
// TODO(lixiaocui) 1.数据库连接异常没有错误处理，需要增加异常处理
//      2.目前sql执行是单线程的，后续考虑增加类似sql连接池的逻辑
// 数据库连接断开，需要重新连接
const int ConnLost = -3;
// repo 内部错误
const int InternalError = -4;

// 数据库基础操作类
class DataBase {
 public:
  DataBase() = default;

  DataBase(const std::string &url,
           const std::string &user,
           const std::string &password);

  ~DataBase();

  /**
   * @brief 通过用户传入的url、user和passwd连接mysql数据库
   * @ return 错误码
   */
  int connectDB();

  /**
   * @ 执行sql语句
   * @ param sql语句字符串
   * @ return 错误码（参见错误码描述）
   */
  int Exec(const std::string &sql);

  /**
   * @ 执行update语句
   * @ param sql语句字符串
   * @ return 错误码（参见错误码描述）
   */
  int ExecUpdate(const std::string &sql);

  /**
   * @ 执行查询sql语句
   * @ param sql语句字符串
   * @ return 错误码（参见错误码描述）
   */
  int QueryRows(const std::string &sql, sql::ResultSet **res);

 private:
  /**
   * @检查连接是否断开
   * @return true（连接有效）/fasle（连接断开）
   */
  bool CheckConn();

 public:
  // mysql服务地址
  std::string url_;
  // 用户名
  std::string user_;
  // 用户密码
  std::string password_;
  // mysql连接对象
  sql::Connection *conn_;
  // mysql执行对象
  sql::Statement *statement_;
  // 保护database操作的互斥锁，目前只提供单线程操作
  std::mutex mutex_;
};
}  // namespace repo
}  // namespace curve

#endif  // SRC_REPO_DATABASE_H_
