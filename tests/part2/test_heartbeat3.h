/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_HEARTBEAT3_H_
#define TESTS_PART2_TEST_HEARTBEAT3_H_

#include <fcntl.h>
#include <sys/stat.h>
#include <cmock.h>
#include <string>
#include "src/part2/common_type.h"
#include "src/part2/config.h"
#include "src/part2/heartbeat.h"
#include "src/part2/rados_interface.h"
class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD4(CheckCmdline, int(int64_t, const char*, char*, int));
};

#endif  // TESTS_PART2_TEST_HEARTBEAT3_H_
