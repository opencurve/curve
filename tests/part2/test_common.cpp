/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_common.h"
CMOCK_MOCK_FUNCTION1(NebdServerMocker, GeneratePort, int(int));
CMOCK_MOCK_FUNCTION2(NebdServerMocker, FindPort, int(config_t*, int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, LockFile, int(const char*));
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidLockfile, std::string());
CMOCK_MOCK_FUNCTION0(NebdServerMocker, Reload, int());
CMOCK_MOCK_FUNCTION1(NebdServerMocker, CheckProc, int(const char*));
CMOCK_MOCK_FUNCTION3(NebdServerMocker, open, int(const char*, int, mode_t));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, ReadQemuXmlDir, std::string(config_t*));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, ReadQemuXml, void(const char*));
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidFile, std::string());
