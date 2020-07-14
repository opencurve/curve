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

/*
 * Project: curve
 * Created Date: Fri Sep 21 2018
 * Author: lixiaocui
 */


#include <gtest/gtest.h>
#include <json/json.h>
#include <mysqlcurve/jdbc/mysql_connection.h>
#include <mysqlcurve/jdbc/cppconn/driver.h>
#include <mysqlcurve/jdbc/cppconn/exception.h>
#include <mysqlcurve/jdbc/cppconn/resultset.h>
#include <mysqlcurve/jdbc/cppconn/statement.h>
#include <mysqlcurve/jdbc/cppconn/prepared_statement.h>

#include "src/repo/repo.h"

/*
 * interface test of connector/c++
*/
namespace curve {
namespace repo {
TEST(MySqlTest, MySqlConn) {
    sql::Driver *driver;
    sql::Connection *conn;
    sql::Statement *statement;

    std::string url = "localhost";
    std::string user1 = "root";
    std::string password1 = "qwer";
    std::string user2 = "curve";
    std::string password2 = "curve";

    {
        try {
            driver = get_driver_instance();
            driver->connect(url, user1, password2 + "hello");
            FAIL();
        } catch (sql::SQLException &e) {
            ASSERT_EQ(1045, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }
    {
        try {
            conn = driver->connect(url, user1, password1);
            statement = conn->createStatement();
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }


    {
        try {
            statement->executeUpdate(
                "drop database if exists curve_mysql_test");
            statement->executeUpdate(
                "create database if not exists curve_mysql_test");
            statement->executeUpdate("use curve_mysql_test");
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->
                executeUpdate(std::string(CreateSnapshotTable) + "hello");
            FAIL();
        } catch (sql::SQLException &e) {
            ASSERT_EQ(1064, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->executeUpdate(CreateSnapshotTable);
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->executeUpdate("");
            FAIL();
        } catch (sql::SQLException &e) {
            ASSERT_EQ(1065, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    std::string insertSql =
        "insert into snapshot (`uuid`,`user`,`filename`,`seqnum`,`chunksize`,`segmentsize`,`filelength`,`time`,`status`,`snapdesc`)" //NOLINT
        "values ('testuuid','curve','test',1,4194304,1073741842,10737418420,9999,0,'mysnap1')";  //NOLINT
    {
        try {
            statement->executeUpdate(insertSql);
            SUCCEED();
        } catch (sql::SQLException &e) {
            std::cout << e.what();
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->executeUpdate(insertSql);
            FAIL();
        } catch (sql::SQLException &e) {
            ASSERT_EQ(1062, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            auto res = statement->executeQuery("select * from snapshot");
            ASSERT_EQ(1, res->rowsCount());
            while (res->next()) {
                ASSERT_EQ("testuuid", res->getString("uuid"));
                ASSERT_EQ("curve", res->getString("user"));
                ASSERT_EQ("test", res->getString("filename"));
                ASSERT_EQ(1, res->getInt64("seqnum"));
                ASSERT_EQ("mysnap1", res->getString("snapdesc"));
            }
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->executeQuery("");
            FAIL();
        } catch (sql::SQLException &e) {
            ASSERT_EQ(1065, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    {
        try {
            statement->executeUpdate("drop database curve_mysql_test");
            delete (conn);
            delete (statement);
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }
}
}  // namespace repo
}  // namespace curve

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

