/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include "tests/part2/test_rados.h"
CMOCK_MOCK_FUNCTION1(NebdServerMocker, CloseRados, void(rados_t *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rados_ioctx_destroy,
                     void(rados_ioctx_t));
CMOCK_MOCK_FUNCTION4(NebdServerMocker, rbd_open,
                     int(rados_ioctx_t, const char *, rbd_image_t *,
                         const char *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rbd_close, int(rbd_image_t));
CMOCK_MOCK_FUNCTION3(NebdServerMocker, rados_ioctx_create,
                     int(rados_t, const char *, rados_ioctx_t *));
CMOCK_MOCK_FUNCTION2(NebdServerMocker, rados_create,
                     int(rados_t *, const char *));
CMOCK_MOCK_FUNCTION2(NebdServerMocker, rados_conf_read_file,
                     int(rados_t, const char *));
CMOCK_MOCK_FUNCTION3(NebdServerMocker, rados_conf_set,
                     int(rados_t, const char *, const char *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rados_connect, int(rados_t));
