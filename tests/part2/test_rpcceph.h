/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#ifndef TESTS_PART2_TEST_RPCCEPH_H_
#define TESTS_PART2_TEST_RPCCEPH_H_

#include <rados/librados.h>
#include <rbd/librbd.h>
#include <cmock.h>
#include <string>
#include "src/part2/config.h"

class NebdServerMocker : public CMockMocker<NebdServerMocker> {
 public:
    MOCK_METHOD5(rbd_aio_write, int(rbd_image_t, uint64_t, size_t, const char *,
                                    rbd_completion_t));
    MOCK_METHOD5(rbd_aio_read,
                 int(rbd_image_t, uint64_t, size_t, char *, rbd_completion_t));
    MOCK_METHOD4(rbd_aio_discard,
                 int(rbd_image_t, uint64_t, uint64_t, rbd_completion_t));
    MOCK_METHOD2(rbd_aio_flush, int(rbd_image_t, rbd_completion_t));
    MOCK_METHOD3(rbd_aio_create_completion,
                 int(void *, rbd_callback_t, rbd_completion_t *));
    MOCK_METHOD1(rbd_invalidate_cache, int(rbd_image_t));
    MOCK_METHOD2(rbd_resize, int(rbd_image_t, uint64_t));
    MOCK_METHOD3(rbd_stat, int(rbd_image_t, rbd_image_info_t *, size_t));
    MOCK_METHOD1(LockFile, int(const char *));
    MOCK_METHOD1(UnLockFile, void(int));
    MOCK_METHOD1(CloseImage, int(int));
    MOCK_METHOD0(GetUuidFile, std::string());
    MOCK_METHOD1(RmFd, int(int));
    MOCK_METHOD1(rbd_aio_get_return_value, ssize_t(rbd_completion_t));
    MOCK_METHOD0(GetUuidLockfile, std::string());
    MOCK_METHOD1(rbd_aio_release, void(rbd_completion_t));
    MOCK_METHOD1(FilenameFdExist, int(char *));
    MOCK_METHOD1(ConnectRados, rados_t *(const char *));
    MOCK_METHOD5(OpenImage,
                 int(rados_t *, const char *, const char *, int, char *));
    MOCK_METHOD1(CloseRados, void(rados_t *));
    MOCK_METHOD2(GenerateFd, int(char *, int));
};
#endif  // TESTS_PART2_TEST_RPCCEPH_H_
