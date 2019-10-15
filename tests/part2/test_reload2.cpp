/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_reload2.h"
CMOCK_MOCK_FUNCTION1(NebdServerMocker, ConnectRados, rados_t*(const char*));
CMOCK_MOCK_FUNCTION5(NebdServerMocker, OpenImage,
                     int(rados_t*, const char*, const char*, int, char*));
