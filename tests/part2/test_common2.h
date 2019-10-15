/*
 *** Project: nebd
 *** File Created: 2019-09-30
 *** Author: hzwuhongsong
 *** Copyright (c) 2019 NetEase
 ***/

#ifndef TESTS_PART2_TEST_COMMON2_H_
#define TESTS_PART2_TEST_COMMON2_H_

#include <cmock.h>
#include "src/part2/config.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD1(LockFile, int(const char*));
    MOCK_METHOD1(unLockFile, void(int));
};

#endif  // TESTS_PART2_TEST_COMMON2_H_
