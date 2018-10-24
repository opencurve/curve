/*
 * Project: curve
 * Created Date: Fri Sep 21 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <json/json.h>
#include <mysqlcurve/jdbc/mysql_connection.h>
#include <mysqlcurve/jdbc/cppconn/driver.h>
#include <mysqlcurve/jdbc/cppconn/exception.h>
#include <mysqlcurve/jdbc/cppconn/resultset.h>
#include <mysqlcurve/jdbc/cppconn/statement.h>
#include <mysqlcurve/jdbc/cppconn/prepared_statement.h>

#include "src/mds/repo/repo.h"

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

    // TEST1: connection error throw SQLException
    {
        try {
            driver = get_driver_instance();
            driver->connect(url, user1, password2 + "hello");
            FAIL();
        } catch (sql::SQLException &e) {
            // access denied code
            ASSERT_EQ(1045, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST2: connect success
    {
        try {
            conn = driver->connect(url, user2, password2);
            statement = conn->createStatement();
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST3: create database fail, permission denied
    {
        try {
            statement->executeUpdate("create database if not exists curve_mds");
            FAIL();
        } catch (sql::SQLException &e) {
            // permission denied code
            ASSERT_EQ(1044, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST4: create database and use success
    {
        try {
            conn = driver->connect(url, user1, password1);
            statement = conn->createStatement();
            // clear exist curve_mds_mysql_test
            statement->executeUpdate(
                "drop database if exists curve_mds_mysql_test");
            statement->executeUpdate(
                "create database if not exists curve_mds_mysql_test");
            statement->executeUpdate("use curve_mds_mysql_test");
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST5: create table error
    {
        try {
            statement->executeUpdate(std::string(CreateZoneTable) + "hello");
            FAIL();
        } catch (sql::SQLException &e) {
            // SQL syntax error code
            ASSERT_EQ(1064, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST6: create table sucess
    {
        try {
            statement->executeUpdate(CreateZoneTable);
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST7: create record in zone with empty sql
    {
        try {
            statement->executeUpdate("");
            FAIL();
        } catch (sql::SQLException &e) {
            // query empty code
            ASSERT_EQ(1065, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    std::string insertSql =
        "insert into curve_zone (`zoneID`,`zoneName`,`poolID`,`desc`) "
        "values (1,'firstzone',1,'first zone')";
    // TEST8: create record in zone success
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

    // TEST9: duplicate insert of primary err
    {
        try {
            statement->executeUpdate(insertSql);
            FAIL();
        } catch (sql::SQLException &e) {
            // Duplicate entry
            ASSERT_EQ(1062, e.getErrorCode());
            SUCCEED();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST10: query from curve_zone
    {
        try {
            auto res = statement->executeQuery("select * from curve_zone");
            ASSERT_EQ(1, res->rowsCount());
            while (res->next()) {
                ASSERT_EQ(1, res->getUInt("zoneID"));
                ASSERT_EQ("firstzone", res->getString("zoneName"));
                ASSERT_EQ(1, res->getUInt("poolID"));
                ASSERT_EQ("first zone", res->getString("desc"));
            }
            SUCCEED();
        } catch (sql::SQLException &e) {
            FAIL();
        } catch (std::runtime_error &e) {
            FAIL();
        }
    }

    // TEST11: query sql error
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

    // End: drop database
    {
        try {
            statement->executeUpdate("drop database curve_mds_mysql_test");
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
