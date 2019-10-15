/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_RADOS_H_
#define TESTS_PART2_TEST_RADOS_H_

#include <rados/librados.h>
#include <rbd/librbd.h>
#include <cmock.h>
#include "src/part2/config.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD1(CloseRados, void(rados_t *));
    MOCK_METHOD1(rados_ioctx_destroy, void(rados_ioctx_t));
    MOCK_METHOD3(rados_ioctx_create,
                 int(rados_t, const char *, rados_ioctx_t *));
    MOCK_METHOD4(rbd_open,
                 int(rados_ioctx_t, const char *, rbd_image_t *, const char *));
    MOCK_METHOD1(rbd_close, int(rbd_image_t));
    MOCK_METHOD2(rados_create, int(rados_t *, const char *));
    MOCK_METHOD2(rados_conf_read_file, int(rados_t, const char *));
    MOCK_METHOD3(rados_conf_set, int(rados_t, const char *, const char *));
    MOCK_METHOD1(rados_connect, int(rados_t));
};

#endif  // TESTS_PART2_TEST_RADOS_H_
