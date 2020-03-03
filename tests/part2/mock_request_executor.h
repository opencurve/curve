/*
 * Project: nebd
 * Created Date: Tuesday January 21st 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_REQUEST_EXECUTOR_H_
#define TESTS_PART2_MOCK_REQUEST_EXECUTOR_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <memory>

#include "src/part2/request_executor.h"

namespace nebd {
namespace server {

class MockFileInstance : public NebdFileInstance {
 public:
    MockFileInstance() {}
    ~MockFileInstance() {}
};

class MockRequestExecutor : public NebdRequestExecutor {
 public:
    MockRequestExecutor() {}
    ~MockRequestExecutor() {}

    MOCK_METHOD1(Open, std::shared_ptr<NebdFileInstance>(const std::string&));
    MOCK_METHOD2(Reopen, std::shared_ptr<NebdFileInstance>(
        const std::string&, const ExtendAttribute&));
    MOCK_METHOD1(Close, int(NebdFileInstance*));
    MOCK_METHOD2(Extend, int(NebdFileInstance*, int64_t));
    MOCK_METHOD2(GetInfo, int(NebdFileInstance*, NebdFileInfo*));
    MOCK_METHOD2(Discard, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(AioRead, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(AioWrite, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(Flush, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD1(InvalidCache, int(NebdFileInstance*));
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_REQUEST_EXECUTOR_H_
