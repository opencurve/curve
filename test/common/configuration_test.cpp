/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/11/23  Wenyu Zhou   Initial version
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include "src/common/configuration.h"

namespace curve {
namespace common {

class ConfigurationTest : public ::testing::Test {
 public:
    void SetUp() {
        std::string confItem;

        confFile_ = "curve.conf.test";
        std::ofstream cFile(confFile_);
        ASSERT_TRUE(cFile.is_open());

        confItem = "test.str1=teststring\n";
        cFile << confItem;

        confItem = "test.int1=12345\n";
        cFile << confItem;

        confItem = "test.int2=-2345\n";
        cFile << confItem;

        confItem = "test.int3=0\n";
        cFile << confItem;

        confItem = "test.bool1=0\n";
        cFile << confItem;

        confItem = "test.bool2=1\n";
        cFile << confItem;

        confItem = "test.bool3=false\n";
        cFile << confItem;

        confItem = "test.bool4=true\n";
        cFile << confItem;

        confItem = "test.bool5=no\n";
        cFile << confItem;

        confItem = "test.bool6=yes\n";
        cFile << confItem;

        confItem = "test.double1=3.1415926\n";
        cFile << confItem;

        confItem = "test.double2=1\n";
        cFile << confItem;

        confItem = "test.double3=1.0\n";
        cFile << confItem;

        confItem = "test.double4=0.1\n";
        cFile << confItem;
    }

    void TearDown() {
        ASSERT_EQ(0, unlink(confFile_.c_str()));
    }

    std::string confFile_;
};

TEST_F(ConfigurationTest, SetAndGetConfigPath) {
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ASSERT_EQ(conf.GetConfigPath(), confFile_);
}

TEST_F(ConfigurationTest, LoadNonExistConfigFile) {
    bool ret;
    std::string confFile = "curve.conf.test.nonexist";
    Configuration conf;

    conf.SetConfigPath(confFile);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, false);
}

TEST_F(ConfigurationTest, LoadNormalConfigFile) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);
}

TEST_F(ConfigurationTest, DumpConfig) {
    Configuration conf;

    conf.SetConfigPath(confFile_);
    // not implemented yet, assert null returned
    ASSERT_EQ(conf.DumpConfig(), "");
}

TEST_F(ConfigurationTest, SaveConfig) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.SaveConfig();
    // not implemented yet, assert false
    ASSERT_EQ(ret, false);
}

TEST_F(ConfigurationTest, GetSetValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetValue("test.str1"), "teststring");
    ASSERT_EQ(conf.GetValue("test.int1"), "12345");
    ASSERT_EQ(conf.GetValue("test.bool1"), "0");
    ASSERT_EQ(conf.GetValue("test.str.nonexist"), "");

    conf.SetValue("test.str1", "teststring2");
    ASSERT_EQ(conf.GetValue("test.str1"), "teststring2");
}

TEST_F(ConfigurationTest, GetSetStringValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetStringValue("test.str1"), "teststring");
    ASSERT_EQ(conf.GetStringValue("test.int1"), "12345");
    ASSERT_EQ(conf.GetStringValue("test.bool1"), "0");
    ASSERT_EQ(conf.GetStringValue("test.str.nonexist"), "");

    conf.SetStringValue("test.str1", "teststring2");
    ASSERT_EQ(conf.GetStringValue("test.str1"), "teststring2");
}

TEST_F(ConfigurationTest, GetSetIntValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetIntValue("test.int1"), 12345);
    ASSERT_EQ(conf.GetIntValue("test.int2"), -2345);
    ASSERT_EQ(conf.GetIntValue("test.int3"), 0);
    ASSERT_EQ(conf.GetIntValue("test.int.nonexist"), 0);

    conf.SetIntValue("test.int1", 123);
    ASSERT_EQ(conf.GetIntValue("test.int1"), 123);
}

TEST_F(ConfigurationTest, GetSetBoolValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetBoolValue("test.bool1"), false);
    ASSERT_EQ(conf.GetBoolValue("test.bool2"), true);
    ASSERT_EQ(conf.GetBoolValue("test.bool3"), false);
    ASSERT_EQ(conf.GetBoolValue("test.bool4"), true);
    ASSERT_EQ(conf.GetBoolValue("test.bool5"), false);
    ASSERT_EQ(conf.GetBoolValue("test.bool6"), true);
    ASSERT_EQ(conf.GetBoolValue("test.bool.nonexist"), false);

    conf.SetBoolValue("test.bool1", true);
    ASSERT_EQ(conf.GetBoolValue("test.bool1"), true);
}

TEST_F(ConfigurationTest, GetSetDoubleValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetDoubleValue("test.double1"), 3.1415926);
    ASSERT_EQ(conf.GetDoubleValue("test.double2"), 1);
    ASSERT_EQ(conf.GetDoubleValue("test.double3"), 1.0);
    ASSERT_EQ(conf.GetDoubleValue("test.double4"), 0.1);

    conf.SetDoubleValue("test.double1", 100.0);
    ASSERT_EQ(conf.GetDoubleValue("test.double1"), 100.0);
}

}  // namespace common
}  // namespace curve
