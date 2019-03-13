/*
 * Project: curve
 * Created Date: 18-10-7
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CLIENT_MOCK_FILE_CLIENT_H_
#define TEST_CLIENT_MOCK_FILE_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>

#include "src/client/libcurve_file.h"

namespace curve {
namespace client {

class MockFileClient : public FileClient {
 public:
    MockFileClient() : FileClient() {}
    ~MockFileClient() = default;

    MOCK_METHOD1(Init, LIBCURVE_ERROR(const char*));
    MOCK_METHOD4(Open, int(const std::string&, UserInfo_t, size_t, bool));
    MOCK_METHOD4(Read, LIBCURVE_ERROR(int, char*, off_t, size_t));
    MOCK_METHOD4(Write, LIBCURVE_ERROR(int, const char*, off_t, size_t));
    MOCK_METHOD2(AioRead, LIBCURVE_ERROR(int, CurveAioContext*));
    MOCK_METHOD2(AioWrite, LIBCURVE_ERROR(int, CurveAioContext*));
    MOCK_METHOD2(StatFs, LIBCURVE_ERROR(int, FileStatInfo*));
    MOCK_METHOD1(Close, void(int));
    MOCK_METHOD0(UnInit, void());
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_FILE_CLIENT_H_
