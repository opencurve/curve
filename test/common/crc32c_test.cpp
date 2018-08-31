/*
 * Project: curve
 * Created Date: 18-8-31
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include <cstring>
#include <chrono>   //NOLINT

#include "src/common/crc32c.h"

class TestTimer {
 public:
    TestTimer() : begin_(std::chrono::high_resolution_clock::now()) {}

    uint64_t elapsed_micro() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - begin_).count();
    }

 private:
    std::chrono::time_point<std::chrono::high_resolution_clock> begin_;
};  // class TestTimer

TEST(Crc32cTest, basic_usage) {
    const char *a = "foo bar baz";
    const char *b = "whiz bang boom";

    ASSERT_EQ(4119623852u, curve::common::CurveCrc32c(0, (unsigned char const *) a, strlen(a)));
    ASSERT_EQ(881700046u, curve::common::CurveCrc32c(1234, (unsigned char const *) a, strlen(a)));
    ASSERT_EQ(2360230088u, curve::common::CurveCrc32c(0, (unsigned char const *) b, strlen(b)));
    ASSERT_EQ(3743019208u, curve::common::CurveCrc32c(5678, (unsigned char const *) b, strlen(b)));
}

TEST(Crc32cTest, huge_buffer) {
    char a[4096000];
    memset(a, 1, 4096000);
    ASSERT_EQ(31583199u, curve::common::CurveCrc32c(0, (unsigned char const *) a, 4096000));
    ASSERT_EQ(1400919119u, curve::common::CurveCrc32c(1234, (unsigned char const *) a, 4096000));
}

TEST(Crc32cTest, partial_word) {
    char a[5];
    char b[35];
    memset(reinterpret_cast<void *>(a), 1, 5);
    memset(reinterpret_cast<void *>(b), 1, 35);
    ASSERT_EQ(2715569182u, curve::common::CurveCrc32c(0, (unsigned char const *) a, 5));
    ASSERT_EQ(440531800u, curve::common::CurveCrc32c(0, (unsigned char const *) b, 35));
}

TEST(Crc32cTest, null_buffer) {
    const char *a = NULL;
    const char *b = nullptr;
    ASSERT_EQ(0, curve::common::CurveCrc32c(0, (unsigned char const *) a, 100));
    ASSERT_EQ(12, curve::common::CurveCrc32c(12, (unsigned char const *) b, 100));
}

TEST(Crc32cTest, simple_performance) {
    TestTimer t;
    size_t loops = 1024 * 1024 * 64;
    const char *a = "1234567890123456";

    for (size_t i = 0; i < loops; ++i) {
        curve::common::CurveCrc32c(0, (unsigned char const *) a, strlen(a));
    }

    double rate = 1.0 / t.elapsed_micro() * 1000 * 1000;
    std::cout << "buffer size: 16B "  "rate: " << rate << "GB/s" << std::endl;

    ASSERT_EQ(1, 1);
}
