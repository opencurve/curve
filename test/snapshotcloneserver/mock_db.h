/*************************************************************************
> File Name: mock_db.h
> Author:
> Created Time: Thu 27 Dec 2018 09:56:04 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/
#ifndef TEST_SNAPSHOTCLONESERVER_MOCK_DB_H_
#define TEST_SNAPSHOTCLONESERVER_MOCK_DB_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/repo/dataBase.h"
#include <string>  //NOLINT
#include <vector>  //NOLINT

using ::testing::Return;
using ::testing::_;
namespace curve {
namespace repo {
class MockDB : public DataBase {
 public:
  MockDB() {}
  ~MockDB() {}

  MOCK_METHOD0(InitDB, int());
  MOCK_METHOD1(Execute, int(const std::string &));
  MOCK_METHOD1(ExecUpdate, int(const std::string &));
  MOCK_METHOD2(QueryRows, int(const std::string &, sql::ResultSet**));
};
}  // namespace repo
}  // namespace curve
#endif  // TEST_SNAPSHOTCLONESERVER_MOCK_DB_H_
