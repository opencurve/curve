/*
 * Project: curve
 * Created Date: Monday May 13th 2019
 * Author: hzsunjianliang
 * Copyright (c) 2019 netease
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <fiu-control.h>
#include "test/failpoint/fiu_local.h"

/*
 * libfiu 使用文档详见：https://blitiri.com.ar/p/libfiu/doc/man-libfiu.html
 * 分为2个部分，一部分是core API，包括fiu_do_on/fiu_return_on/fiu_init
 * core API 用于作用与注入在业务代码处，并由外部control API控制触发。
 * control API 包括：fiu_enable\fiu_disable\fiu_enable_random等等
 * 用于在测试代码处用户进行错误的注入，具体使用方式和方法如下示例代码所示
 */

namespace curve {
namespace failpint {

class FailPointTest: public ::testing::Test {
 protected:
    void SetUp() override {
        fiu_init(0);
    }
    void TearDown() override {
        // noop
    }
};

// 注入方式: 通过返回值的方式进行注入
size_t free_space() {
        fiu_return_on("no_free_space", 0);
        return 100;
}

// 注入方式: 通过side_effet 进行注入
void modify_state(int *val) {
    *val += 1;
    fiu_do_on("side_effect", *val += 1);
    return;
}

// 注入方式: 通过side_effet 进行注入（lambda方式）
void modify_state_with_lamda(int &val) { //NOLINT
    fiu_do_on("side_effect_2",
        auto func = [&] () {
            val++;
        };
        func(););
    return;
}

// 错误触发方式: 总是触发
TEST_F(FailPointTest, alwaysfail) {
    if (fiu_enable("no_free_space", 1, NULL, 0) == 0) {
        ASSERT_EQ(free_space(), 0);
        ASSERT_EQ(free_space(), 0);
    } else {
        ASSERT_EQ(free_space(), 100);
    }
    fiu_disable("no_free_space");
    ASSERT_EQ(free_space(), 100);
}

// 错误触发方式: 随机触发错误
TEST_F(FailPointTest, nondeterministic) {
    if (fiu_enable_random("no_free_space", 1, NULL, 0, 1) == 0) {
        ASSERT_EQ(free_space(), 0);
    }

    if (fiu_enable_random("no_free_space", 1, NULL, 0, 0) == 0) {
        ASSERT_EQ(free_space(), 100);
    }

    if (fiu_enable_random("no_free_space", 1, NULL, 0, 0.5) == 0) {
        int ret = free_space();
        if (ret == 100) {
            ASSERT_TRUE(true) << "failpoint not trigger";
        } else if (ret == 0) {
            ASSERT_TRUE(true) << "failpoint triggered";
        } else {
            FAIL() << "failpint not defineded?";
        }
    }
}

TEST_F(FailPointTest, sideEffect) {
    if (fiu_enable("side_effect", 1, NULL, 0) == 0) {
        int val = 0;
        modify_state(&val);
        ASSERT_EQ(val, 2);
    }

    if (fiu_enable("side_effect_2", 1, NULL, 0) == 0) {
        int val = 0;
        modify_state_with_lamda(val);
        ASSERT_EQ(val, 1);
    }
}

bool IoRead(void) {
    fiu_return_on("chunkserver/copyset/read", false);
    return true;
}

bool IoWrite(void) {
    fiu_return_on("chunkserver/copyset/write", false);
    return true;
}

bool IoDelete(void) {
    fiu_return_on("chunkserver/copyset/delete", false);
    return true;
}

TEST_F(FailPointTest, WildZard) {
    if (fiu_enable("chunkserver/copyset/*", 1, NULL, 0) == 0) {
        ASSERT_FALSE(IoRead());
        ASSERT_FALSE(IoWrite());
        ASSERT_FALSE(IoDelete());
    }
    if (fiu_disable("chunkserver/copyset/*") == 0) {
        ASSERT_TRUE(IoRead());
        ASSERT_TRUE(IoWrite());
        ASSERT_TRUE(IoDelete());
    }
}


}  // namespace failpint
}  // namespace curve
