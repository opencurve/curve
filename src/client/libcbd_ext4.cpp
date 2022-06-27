/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 *
 * History:
 *          2018/10/10  Wenyu Zhou   Initial version
 */

#include "include/client/libcbd.h"
#include "include/client/libcurve.h"

extern "C" {

CurveOptions g_cbd_ext4_options = {0};

int cbd_ext4_init(const CurveOptions* options) {
    if (g_cbd_ext4_options.inited) {
        return 0;
    }
    g_cbd_ext4_options.conf = options->conf;

#ifdef CBD_BACKEND_EXT4
    g_cbd_ext4_options.datahome = options->datahome;
    if (!g_cbd_ext4_options.datahome) {
        return -1;
    }
#endif

    g_cbd_ext4_options.inited = true;
    return 0;
}

int cbd_ext4_fini() {
    return 0;
}

int cbd_ext4_open(const char* filename) {
    int fd = -1;
    char path[CBD_MAX_FILE_PATH_LEN] = {0};
#ifdef CBD_BACKEND_EXT4
    strcat(path, g_cbd_ext4_options.datahome);  //NOLINT
    strcat(path, "/");  //NOLINT
#endif
    strcat(path, filename);    //NOLINT

    fd = open(path, O_RDWR | O_CREAT, 0660);

    return fd;
}

int cbd_ext4_close(int fd) {
    return close(fd);
}

int cbd_ext4_pread(int fd, void* buf, off_t offset, size_t length) {
    return pread(fd, buf, length, offset);
}

int cbd_ext4_pwrite(int fd, const void* buf, off_t offset, size_t length) {
    return pwrite(fd, buf, length, offset);
}

void cbd_ext4_aio_callback(union sigval sigev_value) {
    CurveAioContext* context = (CurveAioContext *)sigev_value.sival_ptr;    //NOLINT
    context->cb(context);
}

int cbd_ext4_aio_pread(int fd, CurveAioContext* context) {
    struct aiocb* cb;

    cb = (struct aiocb *)malloc(sizeof(struct aiocb));
    if (!cb) {
        return -1;
    }

    memset(cb, 0, sizeof(struct aiocb));
    cb->aio_fildes = fd;
    cb->aio_offset = context->offset;
    cb->aio_nbytes = context->length;
    cb->aio_buf = context->buf;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_value.sival_ptr = (void*)context;    //NOLINT
    cb->aio_sigevent.sigev_notify_function = cbd_ext4_aio_callback;

    return aio_read(cb);
}

int cbd_ext4_aio_pwrite(int fd, CurveAioContext* context) {
    struct aiocb* cb;

    cb = (struct aiocb *)malloc(sizeof(struct aiocb));
    if (!cb) {
        return -1;
    }

    memset(cb, 0, sizeof(struct aiocb));
    cb->aio_fildes = fd;
    cb->aio_offset = context->offset;
    cb->aio_nbytes = context->length;
    cb->aio_buf = context->buf;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_value.sival_ptr = (void*)context;    //NOLINT
    cb->aio_sigevent.sigev_notify_function = cbd_ext4_aio_callback;

    return aio_write(cb);
}

int cbd_ext4_sync(int fd) {
    return fsync(fd);
}

int64_t cbd_ext4_filesize(const char* filename) {
    struct stat st;
    int ret;

    char path[CBD_MAX_FILE_PATH_LEN] = {0};
#ifdef CBD_BACKEND_EXT4
    strcat(path, g_cbd_ext4_options.datahome);  //NOLINT
    strcat(path, "/");  //NOLINT
#endif
    strcat(path, filename);    //NOLINT

    ret = stat(path, &st);
    if (ret) {
        return ret;
    } else {
        return st.st_size;
    }
}

int cbd_ext4_increase_epoch(const char* filename) {
    return 0;
}

}  // extern "C"

