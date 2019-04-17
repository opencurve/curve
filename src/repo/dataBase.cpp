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
                   const std::string &password) {
    this->url_ = url;
    this->user_ = user;
    this->password_ = password;
}

DataBase::~DataBase() {
    delete statement_;
    delete conn_;
}

int DataBase::connectDB() {
    try {
        /*
         * get_driver_instance() is not thread-safe.
         * Either avoid invoking these methods from within multiple threads
         * at once, or surround the calls with a mutex to prevent simultaneous
         * execution in multiple threads.
         * Make sure that you free con, the sql::Connection object,
         * as soon as you do not need it any more. But do not explicitly
         * free driver, the connector object.
         * Connector/C++ takes care of freeing that.
         */
        sql::Driver *driver;
        driver = get_driver_instance();
        conn_ = driver->connect(url_, user_, password_);
        statement_ = conn_->createStatement();
        return OperationOK;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "connect fail, "
                   << "error code: " << e.getErrorCode() << ", "
                   << "error message: " << e.what();
        return SqlException;
    } catch (std::runtime_error &e) {
        LOG(ERROR) << "[dataBase.cpp] connect db get runtime_error, "
                   << "error message: " << e.what();
        return RuntimeExecption;
    }
}

// retrun value: rows affected
int DataBase::ExecUpdate(const std::string &sql) {
    std::lock_guard<std::mutex> guard(mutex_);
    try {
        if (!CheckConn()) {
            return ConnLost;
        }

        statement_->executeUpdate(sql);
        return OperationOK;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "execUpdate sql: " << sql << "get sqlException, "
                   << "error code: " << e.getErrorCode() << ", "
                   << "error message: " << e.what();
        return SqlException;
    } catch (std::runtime_error &e) {
        LOG(ERROR) << "execUpdate sql: " << sql << "get runtime_error, "
                   << "error message: " << e.what();
        return RuntimeExecption;
    }
}

// return queryResult
int DataBase::QueryRows(const std::string &sql, sql::ResultSet **res) {
    assert(res != nullptr);
    std::lock_guard<std::mutex> guard(mutex_);
    try {
        if (!CheckConn()) {
            return ConnLost;
        }

        *res = statement_->executeQuery(sql);
        return OperationOK;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "queryRows sql: " << sql << "get sqlException, "
                   << "error code: " << e.getErrorCode() << ", "
                   << "error message: " << e.what();
        return SqlException;
    } catch (std::runtime_error &e) {
        LOG(ERROR) << "queryRows sql: " << sql << "get runtime_error, "
                   << "error message: " << e.what();
        return RuntimeExecption;
    }
}

bool DataBase::CheckConn() {
    auto res = conn_->isValid() && !conn_->isClosed();
    if (!res) {
        LOG(ERROR) << "database connect situation false";
    }
    return res;
}
}  // namespace repo
}  // namespace curve
