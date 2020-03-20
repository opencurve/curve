/*
 * Project: curve
 * Created Date: Mon Sep 10 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/repo/dataBase.h"

namespace curve {
namespace repo {
DataBase::DataBase(const std::string &user,
                   const std::string &url,
                   const std::string &password,
                   const std::string &schema,
                   uint32_t capacity) {
    this->url_ = url;
    this->user_ = user;
    this->password_ = password;
    this->connPoolCapacity_ = capacity;
    this->schema_ = schema;
}

DataBase::~DataBase() {
    connPool_ = nullptr;
}

int DataBase::InitDB() {
    connPool_ = ConnPool::GetInstance(
                url_, user_, password_, connPoolCapacity_);
    if (connPool_ == nullptr) {
        LOG(ERROR) << "Init Database failed!";
        return InternalError;
    }
    return OperationOK;
}

int DataBase::Execute(const std::string &sql) {
    sql::Connection *conn = nullptr;
    sql::Statement *statement = nullptr;
    conn = connPool_->GetConnection();
    if (conn) {
        try {
              statement = conn->createStatement();
              statement->execute(sql);
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return OperationOK;
        } catch (sql::SQLException &e) {
              LOG(ERROR) << "execute sql: " << sql << "get sqlException, "
                         << "error code: " << e.getErrorCode() << ", "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return SqlException;
        } catch (std::runtime_error &e) {
              LOG(ERROR) << "execute sql: " << sql << "get runtime_error, "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return RuntimeExecption;
        }
    } else {
        LOG(ERROR) << "Can not get connection, please check whether the "
                   << "database is started or the connection pool size conf "
                   << "is too small";
        return InternalError;
    }
}


// retrun value: rows affected
int DataBase::ExecUpdate(const std::string &sql) {
    sql::Connection *conn = nullptr;
    sql::Statement *statement = nullptr;
    conn = connPool_->GetConnection();
    if (conn) {
        try {
              conn->setSchema(schema_);
              statement = conn->createStatement();
              statement->executeUpdate(sql);
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return OperationOK;
        } catch (sql::SQLException &e) {
              LOG(ERROR) << "execUpdate sql: " << sql << "get sqlException, "
                         << "error code: " << e.getErrorCode() << ", "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return SqlException;
        } catch (std::runtime_error &e) {
              LOG(ERROR) << "execUpdate sql: " << sql << "get runtime_error, "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return RuntimeExecption;
        }
    } else {
        LOG(ERROR) << "Can not get connection, please check whether the "
                   << "database is started or the connection pool size conf "
                   << "is too small";
        return InternalError;
    }
}

// return queryResult
int DataBase::QueryRows(const std::string &sql, sql::ResultSet **res) {
    assert(res != nullptr);
    sql::Connection *conn = nullptr;
    sql::Statement *statement = nullptr;
    conn = connPool_->GetConnection();
    if (conn) {
        try {
              conn->setSchema(schema_);
              statement = conn->createStatement();
              *res = statement->executeQuery(sql);
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return OperationOK;
        } catch (sql::SQLException &e) {
              LOG(ERROR) << "queryRows sql: " << sql << "get sqlException, "
                         << "error code: " << e.getErrorCode() << ", "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return SqlException;
        } catch (std::runtime_error &e) {
              LOG(ERROR) << "queryRows sql: " << sql << "get runtime_error, "
                         << "error message: " << e.what();
              delete statement;
              statement = nullptr;
              connPool_->PutConnection(conn);
              return RuntimeExecption;
        }
    } else {
        LOG(ERROR) << "Can not get connection, please check whether the "
                   << "database is started or the connection pool size conf "
                   << "is too small";
        return InternalError;
    }
}
}  // namespace repo
}  // namespace curve
