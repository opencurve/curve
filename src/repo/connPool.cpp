/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/* ====================================================
#
#   Author        : hzzhaojianming
#   Email         : hzzhaojianming@corp.netease.com
#   File Name     : connPool.cpp
#   Last Modified : 2019-04-24 19:18
#   Describe      :
#
# ====================================================*/

#include "src/repo/connPool.h"
#include <glog/logging.h>
namespace curve {
namespace repo {
const int default_conn_pool_size = 8;
const int capacity = 16;
ConnPool* ConnPool::connPool_ = nullptr;
ConnPool::ConnPool(const std::string &url,
                   const std::string &user,
                   const std::string &passwd,
                   uint32_t capacity) {
    size_ = 0;
    url_ = url;
    user_ = user;
    passwd_ = passwd;
    capacity_ = capacity;
    // get_driver_instance非线程安全，避免多线程并发调用
    try {
        driver_ = get_driver_instance();
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "DB get driver fail, "
                   << "error code: " << e.getErrorCode() << ", "
                   << "error message: " << e.what();
    } catch (std::runtime_error &e) {
        LOG(ERROR) << "DB get driver fail, "
                   << "error message: " << e.what();
    }
    InitConnPool(default_conn_pool_size);
}
ConnPool::~ConnPool() {
    DeInitConnPool();
    connPool_ = nullptr;
}
sql::Connection* ConnPool::CreateConnection() {
    sql::Connection *conn;
    try {
        conn = driver_->connect(url_, user_, passwd_);
        return conn;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "Create connection failed, "
                   << "error code: " << e.getErrorCode() << ", "
                   << "error message: " << e.what();
        return nullptr;
    } catch (std::runtime_error &e) {
        LOG(ERROR) << "Create connection get runtime error, "
                   << "error message: " << e.what();
        return nullptr;
    }
}

void ConnPool::DestroyConnection(sql::Connection *conn) {
    if (conn) {
        try {
          conn->close();
        } catch (sql::SQLException &e) {
          LOG(ERROR) << "Close connection failed, "
                     << "error code: " << e.getErrorCode() << ", "
                     << "error message: " << e.what();
        }
        delete conn;
    }
}

void ConnPool::InitConnPool(int size) {
    LOG(INFO) << "Init DB connection pool with size: "
              << size;
    sql::Connection *conn;
    for (int i = 0; i < size; i++) {
        conn = CreateConnection();
        if (conn) {
            connList_.push_back(conn);
            ++size_;
        } else {
            LOG(ERROR) << "Create connection failed";
        }
    }
}

void ConnPool::DeInitConnPool() {
    LOG(INFO) << "Deinit connection pool";
    std::list<sql::Connection*>::iterator it;
    for (it = connList_.begin(); it != connList_.end(); ++it) {
        DestroyConnection(*it);
    }
    size_ = 0;
    connList_.clear();
}

sql::Connection* ConnPool::GetConnection() {
    sql::Connection *conn;
    std::lock_guard<curve::common::Mutex> guard(mutex_);
    // 队列中有元素，从队列头拿一个连接
    if (connList_.size() > 0) {
        conn = connList_.front();
        connList_.pop_front();
        // 如果连接已经关闭，重新打开一个
        if (conn->isClosed()||!conn->isValid()) {
            delete conn;
            conn = CreateConnection();
            LOG(INFO) << "delete conn and create one = " << conn;
        }
        if (conn == nullptr) {
            --size_;
        }
        return conn;
    } else {
      // 队列中没有连接，创建的连接数小于最大可创建连接数
        if (size_ < capacity_) {
            conn = CreateConnection();
            if (conn) {
                ++size_;
            }
            LOG(INFO) << "size < capacity_, create one, now size = " << size_
                      << ", return conn = " << conn;
            return conn;
        } else {
            LOG(INFO) << "size_(" << size_
                      << ") >= capacity_(" << capacity_
                      << "), return nullptr";
            return nullptr;
        }
    }
}

void ConnPool::PutConnection(sql::Connection* conn) {
    std::lock_guard<curve::common::Mutex> guard(mutex_);
    if (conn) {
        connList_.push_back(conn);
    }
}

ConnPool* ConnPool::GetInstance(const std::string &url,
                                const std::string &user,
                                const std::string &passwd,
                                uint32_t capacity) {
    if (connPool_ == nullptr) {
        connPool_ = new ConnPool(url, user, passwd, capacity);
    }
    return connPool_;
}
}  // namespace repo
}  // namespace curve
