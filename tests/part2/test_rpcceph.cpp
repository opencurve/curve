/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <string>
#include "tests/part2/test_rpcceph.h"

CMOCK_MOCK_FUNCTION2(NebdServerMocker, GenerateFd, int(char *, int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, CloseRados, void(rados_t *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, FilenameFdExist, int(char *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, ConnectRados, rados_t *(const char *));
CMOCK_MOCK_FUNCTION5(NebdServerMocker, OpenImage,
                     int(rados_t *, const char *, const char *, int, char *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rbd_aio_release, void(rbd_completion_t));
CMOCK_MOCK_FUNCTION5(NebdServerMocker, rbd_aio_read,
                     int(rbd_image_t, uint64_t, size_t, char *,
                         rbd_completion_t));
CMOCK_MOCK_FUNCTION4(NebdServerMocker, rbd_aio_discard,
                     int(rbd_image_t, uint64_t, uint64_t, rbd_completion_t));
CMOCK_MOCK_FUNCTION2(NebdServerMocker, rbd_aio_flush,
                     int(rbd_image_t, rbd_completion_t));
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidLockfile, std::string());
CMOCK_MOCK_FUNCTION1(NebdServerMocker, LockFile, int(const char *));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rbd_aio_get_return_value,
                     ssize_t(rbd_completion_t));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, UnLockFile, void(int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, CloseImage, int(int));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, RmFd, int(int));
CMOCK_MOCK_FUNCTION0(NebdServerMocker, GetUuidFile, std::string());
CMOCK_MOCK_FUNCTION5(NebdServerMocker, rbd_aio_write,
                     int(rbd_image_t, uint64_t, size_t, const char *,
                         rbd_completion_t));
CMOCK_MOCK_FUNCTION3(NebdServerMocker, rbd_aio_create_completion,
                     int(void *, rbd_callback_t, rbd_completion_t *));
CMOCK_MOCK_FUNCTION3(NebdServerMocker, rbd_stat,
                     int(rbd_image_t, rbd_image_info_t *, size_t));
CMOCK_MOCK_FUNCTION2(NebdServerMocker, rbd_resize, int(rbd_image_t, uint64_t));
CMOCK_MOCK_FUNCTION1(NebdServerMocker, rbd_invalidate_cache, int(rbd_image_t));
