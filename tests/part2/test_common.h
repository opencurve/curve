/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_COMMON_H_
#define TESTS_PART2_TEST_COMMON_H_

#include <cmock.h>
#include <string>
#include "src/part2/config.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    // MOCK_METHOD0(InitConfig, config_t*());
    MOCK_METHOD1(ReadQemuXmlDir, std::string(config_t *));
    MOCK_METHOD1(ReadQemuXml, void(const char *));
    MOCK_METHOD0(GetUuidFile, std::string());
    MOCK_METHOD0(GetUuidLockfile, std::string());
    MOCK_METHOD3(open, int(const char *, int, mode_t));
    MOCK_METHOD1(CheckProc, int(const char *));
    MOCK_METHOD1(LockFile, int(const char *));
    MOCK_METHOD0(Reload, int());
    MOCK_METHOD2(FindPort, int(config_t *, int));
    MOCK_METHOD1(GeneratePort, int(int));
};

#endif  // TESTS_PART2_TEST_COMMON_H_
