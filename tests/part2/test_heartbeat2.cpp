/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_heartbeat2.h"
CMOCK_MOCK_FUNCTION3(NebdServerMocker, open, int(const char*, int, mode_t));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, LockFile, int(const char*));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, UnLockFile, void(int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, CloseImage, int(int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, RmFd, int(int));
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidFile, std::string());
CMOCK_MOCK_FUNCTION0(NebdServerMocker, ReadQemuXmls, int());
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidLockfile, std::string());
