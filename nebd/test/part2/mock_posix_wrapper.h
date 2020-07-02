/*
 * Project: nebd
 * Created Date: 2020-01-21
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_POSIX_WRAPPER_H_
#define TESTS_PART2_MOCK_POSIX_WRAPPER_H_

#include <gmock/gmock.h>
#include "nebd/src/common/posix_wrapper.h"

namespace nebd {
namespace common {

class MockPosixWrapper : public PosixWrapper {
 public:
    MOCK_METHOD3(open, int(const char *, int, mode_t));
    MOCK_METHOD1(close, int(int));
    MOCK_METHOD1(remove, int(const char *));
    MOCK_METHOD2(rename, int(const char *, const char *));
    MOCK_METHOD4(pwrite, ssize_t(int fd, const void *buf,
                                 size_t count, off_t offset));
};

}   // namespace common
}   // namespace nebd

#endif  // TESTS_PART2_MOCK_POSIX_WRAPPER_H_
