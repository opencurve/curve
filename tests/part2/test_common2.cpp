/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_common2.h"
CMOCK_MOCK_FUNCTION1(NebdServerMocker, LockFile, int(const char*));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, unLockFile, void(int));
