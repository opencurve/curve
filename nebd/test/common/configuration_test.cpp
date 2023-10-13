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
 * Project: Curve
 *
 * History:
 *          2018/11/23  Wenyu Zhou   Initial version
 */

#include "nebd/src/common/configuration.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace nebd {
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

    void TearDown() { ASSERT_EQ(0, unlink(confFile_.c_str())); }

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

TEST_F(ConfigurationTest, ListConfig) {
    Configuration conf;

    conf.SetConfigPath(confFile_);
    int ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);
    std::map<std::string, std::string> configs;
    configs = conf.ListConfig();
    ASSERT_NE(0, configs.size());
    // Pick a few keys for validation.
    ASSERT_EQ(configs["test.int1"], "12345");
    ASSERT_EQ(configs["test.bool1"], "0");
    // If the key does not exist, return empty
    ASSERT_EQ(configs["xxx"], "");
}

TEST_F(ConfigurationTest, SaveConfig) {
    bool ret;
    Configuration conf;
    conf.SetConfigPath(confFile_);

    // Customize configuration items and save them
    conf.SetStringValue("test.str1", "new");
    ret = conf.SaveConfig();
    ASSERT_EQ(ret, true);

    // Reload Configuration Items
    Configuration conf2;
    conf2.SetConfigPath(confFile_);
    ret = conf2.LoadConfig();
    ASSERT_EQ(ret, true);

    // Custom configuration items can be read, but the original configuration
    // items are overwritten and cannot be read
    ASSERT_EQ(conf2.GetValue("test.str1"), "new");
    ASSERT_EQ(conf2.GetValue("test.int1"), "");
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
    std::string out;
    ASSERT_FALSE(conf.GetValue("no.exist", &out));
    conf.SetValue("put.in", "put.in");
    ASSERT_TRUE(conf.GetValue("put.in", &out));
    ASSERT_EQ("put.in", out);
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

    std::string out;
    ASSERT_FALSE(conf.GetStringValue("no.exist", &out));
    conf.SetStringValue("put.in", "put.in");
    ASSERT_TRUE(conf.GetStringValue("put.in", &out));
    ASSERT_EQ("put.in", out);
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

    int out;
    ASSERT_FALSE(conf.GetIntValue("no.exist", &out));
    conf.SetIntValue("no.exist", 1);
    ASSERT_TRUE(conf.GetIntValue("no.exist", &out));
    ASSERT_EQ(1, out);

    uint32_t outu32;
    ASSERT_FALSE(conf.GetUInt32Value("no.exist.u32", &outu32));
    conf.SetIntValue("no.exist.u32", 2);
    ASSERT_TRUE(conf.GetUInt32Value("no.exist.u32", &outu32));
    ASSERT_EQ(2, outu32);

    uint64_t outu64;
    ASSERT_FALSE(conf.GetUInt64Value("no.exist.u64", &outu64));
    conf.SetIntValue("no.exist.u64", 3);
    ASSERT_TRUE(conf.GetUInt64Value("no.exist.u64", &outu64));
    ASSERT_EQ(3, outu64);
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

    bool out;
    ASSERT_FALSE(conf.GetBoolValue("no.exist", &out));
    conf.SetIntValue("no.exist", false);
    ASSERT_TRUE(conf.GetBoolValue("no.exist", &out));
    ASSERT_FALSE(out);
}

TEST_F(ConfigurationTest, GetSetDoubleAndFloatValue) {
    bool ret;
    Configuration conf;

    conf.SetConfigPath(confFile_);
    ret = conf.LoadConfig();
    ASSERT_EQ(ret, true);

    ASSERT_EQ(conf.GetDoubleValue("test.double1"), 3.1415926);
    ASSERT_EQ(conf.GetDoubleValue("test.double2"), 1);
    ASSERT_EQ(conf.GetDoubleValue("test.double3"), 1.0);
    ASSERT_EQ(conf.GetDoubleValue("test.double4"), 0.1);
    ASSERT_EQ(conf.GetFloatValue("test.double4"), 0.1f);

    conf.SetDoubleValue("test.double1", 100.0);
    ASSERT_EQ(conf.GetDoubleValue("test.double1"), 100.0);

    double out;
    float outf;
    ASSERT_FALSE(conf.GetDoubleValue("no.exist", &out));
    ASSERT_FALSE(conf.GetFloatValue("no.exist", &outf));
    conf.SetDoubleValue("no.exist", 0.009);
    ASSERT_TRUE(conf.GetDoubleValue("no.exist", &out));
    ASSERT_TRUE(conf.GetFloatValue("no.exist", &outf));
    ASSERT_EQ(0.009, out);
    ASSERT_EQ(0.009f, outf);
}

}  // namespace common
}  // namespace nebd

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    int ret = RUN_ALL_TESTS();

    return ret;
}
