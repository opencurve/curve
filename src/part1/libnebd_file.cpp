/*
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "src/part1/libnebd_file.h"
#include <string>
#include "src/common/configuration.h"
#include "src/part1/nebd_lifecycle.h"
#include "src/part1/nebd_client.h"


int Init4Qemu(const char* confpath) {
    return nebd::client::fileClient.Init(confpath);
}

void Uninit4Qemu() {
    nebd::client::fileClient.Uninit();
    return;
}

int Open4Qemu(const char* filename) {
    return nebd::client::fileClient.Open(filename);
}

int Close4Qemu(int fd) {
    return nebd::client::fileClient.Close(fd);
}

int Extend4Qemu(int fd, int64_t newsize) {
    return nebd::client::fileClient.Extend(fd, newsize);
}

int64_t StatFile4Qemu(int fd) {
    return nebd::client::fileClient.StatFile(fd);
}

int Discard4Qemu(int fd, ClientAioContext* aioctx) {
    return nebd::client::fileClient.Discard(fd, aioctx);
}

int AioRead4Qemu(int fd, ClientAioContext* aioctx) {
    return nebd::client::fileClient.AioRead(fd, aioctx);
}

int AioWrite4Qemu(int fd, ClientAioContext* aioctx) {
    return nebd::client::fileClient.AioWrite(fd, aioctx);
}

int Flush4Qemu(int fd, ClientAioContext* aioctx) {
    return nebd::client::fileClient.Flush(fd, aioctx);
}

int64_t GetInfo4Qemu(int fd) {
    return nebd::client::fileClient.GetInfo(fd);
}

int InvalidCache4Qemu(int fd) {
    return nebd::client::fileClient.InvalidCache(fd);
}
