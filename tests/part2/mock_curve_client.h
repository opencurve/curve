/*
 * Project: nebd
 * Created Date: 2020-02-05
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#ifndef TEST_MOCK_CURVE_CLIENT_H_
#define TEST_MOCK_CURVE_CLIENT_H_

#include <libcurve.h>
#include <gmock/gmock.h>
#include <string>

namespace nebd {
namespace server {
class MockCurveClient : public ::curve::client::CurveClient {
 public:
    MockCurveClient() {}
    ~MockCurveClient() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD2(Open, int(const std::string&, std::string*));
    MOCK_METHOD3(ReOpen, int(
        const std::string&, const std::string&, std::string*));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD2(Extend, int(const std::string&, int64_t));
    MOCK_METHOD1(StatFile, int64_t(const std::string&));
    MOCK_METHOD2(AioRead, int(int, CurveAioContext*));
    MOCK_METHOD2(AioWrite, int(int, CurveAioContext*));
};

}  // namespace server
}  // namespace nebd

#endif  // TEST_MOCK_CURVE_CLIENT_H_
