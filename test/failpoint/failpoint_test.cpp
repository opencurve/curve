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
 * Created Date: Monday May 13th 2019
 * Author: hzsunjianliang
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <fiu-control.h>
#include "test/failpoint/fiu_local.h"

/*
 * For detailed documentation on how to use libfiu, please refer to: https://blitiri.com.ar/p/libfiu/doc/man-libfiu.html
 * Libfiu is divided into two parts: the core API, which includes functions like fiu_do_on/fiu_return_on/fiu_init.
 * The core API is used to inject faults into your business code and is controlled externally using the control API.
 * The control API includes functions like fiu_enable, fiu_disable, fiu_enable_random, and more.
 * These functions are used in your test code to inject errors. You can find specific usage examples and methods in the code snippets below.
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

// Injection method: Inject by returning a value
size_t free_space() {
        fiu_return_on("no_free_space", 0);
        return 100;
}

// Injection method: through side_effet injection
void modify_state(int *val) {
    *val += 1;
    fiu_do_on("side_effect", *val += 1);
    return;
}

// Injection method: through side_effet injection (lambda method)
void modify_state_with_lamda(int &val) { //NOLINT
    fiu_do_on("side_effect_2",
        auto func = [&] () {
            val++;
        };
        func(););
    return;
}

// Error triggering method: always triggered
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

// Error triggering method: Random error triggering
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
