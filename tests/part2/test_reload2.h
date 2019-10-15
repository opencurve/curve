/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_RELOAD2_H_
#define TESTS_PART2_TEST_RELOAD2_H_

#include <rados/librados.h>
#include <rbd/librbd.h>
#include <cmock.h>
#include "src/part2/rados_interface.h"
#include "src/part2/reload.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD1(ConnectRados, rados_t*(const char*));
    MOCK_METHOD5(OpenImage,
                 int(rados_t*, const char*, const char*, int, char*));
};

#endif  // TESTS_PART2_TEST_RELOAD2_H_
