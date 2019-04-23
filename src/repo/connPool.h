/* ====================================================
#   Copyright (C)2019 Netease All rights reserved.
#
#   Author        : hzzhaojianming
#   Email         : hzzhaojianming@corp.netease.com
#   File Name     : connPool.h
#   Last Modified : 2019-04-24 14:16
#   Describe      :
#
# ====================================================*/
#ifndef SRC_REPO_CONNPOOL_H_
#define SRC_REPO_CONNPOOL_H_

#include <mysqlcurve/jdbc/mysql_connection.h>
#include <mysqlcurve/jdbc/cppconn/driver.h>
#include <mysqlcurve/jdbc/cppconn/exception.h>
#include <mysqlcurve/jdbc/cppconn/resultset.h>
#include <mysqlcurve/jdbc/cppconn/statement.h>
#include <mysqlcurve/jdbc/cppconn/prepared_statement.h>
#include <string>
#include <mutex> //NOLINT
#include <list>
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace repo {
class ConnPool {
 public:
   /**
    * @brief 获取连接池中的一个连接
    * @return sql连接对象
    */
    sql::Connection* GetConnection();
    /**
     * @brief 将一个连接交回连接池
     * @param sql连接对象
     * @return 无
     */
    void PutConnection(sql::Connection* conn);
    /**
     * @brief 单例模式，获取ConnPool对象
     * @return ConnPool对象
     */
    static ConnPool* GetInstance(const std::string &url,
                                 const std::string &user,
                                 const std::string &passwd,
                                 uint32_t capacity);
    ~ConnPool();

 private:
    ConnPool(const std::string &url,
             const std::string &user,
             const std::string &passwd,
             uint32_t capacity);
    /**
     * @brief 创建一个数据库连接
     * @return 数库据连接对象指针
     */
    sql::Connection* CreateConnection();
    /**
     * @brief 销毁一个连接对象
     * @param 数据库连接对象指针
     * @return
     */
    void DestroyConnection(sql::Connection* conn);
    /**
     * @brief 初始化一个指定size大小的连接池
     * @param 初始化连接池的连接个数
     * @return
     */
    void InitConnPool(int size);
    /**
     * @brief 销毁连接池
     * @return
     */
    void DeInitConnPool();
    // 数据库连接池对象
    static ConnPool *connPool_;
    // 连接池可创建的最大连接数
    uint32_t capacity_;
    // 当前已经创建的连接数
    uint32_t size_;
    // 连接池链表
    std::list<sql::Connection*> connList_;
    // 保护连接池链表的互斥锁
    curve::common::Mutex mutex_;
    // 数据库服务地址
    std::string url_;
    // 数据库用户名
    std::string user_;
    // 数据库口令
    std::string passwd_;
    // 数据库连接驱动
    sql::Driver *driver_;
};
}  // namespace repo
}  // namespace curve
#endif  // SRC_REPO_CONNPOOL_H_



