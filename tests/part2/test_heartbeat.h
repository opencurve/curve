/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_HEARTBEAT_H_
#define TESTS_PART2_TEST_HEARTBEAT_H_

#include <cmock.h>
#include <string>
#include "src/part2/common_type.h"
#include "src/part2/config.h"
#include "src/part2/heartbeat.h"
#include "src/part2/rados_interface.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD1(LockFile, int(const char*));
    MOCK_METHOD1(UnLockFile, void(int));
    MOCK_METHOD1(CloseImage, int(int));
    MOCK_METHOD0(GetUuidFile, std::string());
    MOCK_METHOD0(GetUuidLockfile, std::string());
    MOCK_METHOD1(CheckProc, int(const char*));
    MOCK_METHOD1(RmFd, int(int));
    MOCK_METHOD0(ReadQemuXmls, int());
    MOCK_METHOD1(CloseQemuDetachedVolumes, int(int));
};

#endif  // TESTS_PART2_TEST_HEARTBEAT_H_
