/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_heartbeat3.h"
CMOCK_MOCK_FUNCTION4(NebdServerMocker, CheckCmdline,
                     int(int64_t, const char*, char*, int));
