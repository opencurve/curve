/*
 * Project: curve
 * Created Date: 18-10-7
 * Author: wudemiao
 * Copyright(c) 2018 netease
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

    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD2(Open, int(const std::string&, const UserInfo_t&));
    MOCK_METHOD4(Read, int(int, char*, off_t, size_t));
    MOCK_METHOD4(Write, int(int, const char*, off_t, size_t));
    MOCK_METHOD2(AioRead, int(int, CurveAioContext*));
    MOCK_METHOD2(AioWrite, int(int, CurveAioContext*));
    MOCK_METHOD3(StatFile, int(const std::string&,
                               const UserInfo_t&,
                               FileStatInfo*));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD0(UnInit, void());
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_FILE_CLIENT_H_
