/*
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "nebd/src/part1/libnebd_file.h"
#include <string>
#include "src/common/configuration.h"
#include "nebd/src/part1/nebd_client.h"


int Init4Nebd(const char* confpath) {
    return nebd::client::nebdClient.Init(confpath);
}

void Uninit4Nebd() {
    nebd::client::nebdClient.Uninit();
    return;
}

int Open4Nebd(const char* filename) {
    return nebd::client::nebdClient.Open(filename);
}

int Close4Nebd(int fd) {
    return nebd::client::nebdClient.Close(fd);
}

int Extend4Nebd(int fd, int64_t newsize) {
    return nebd::client::nebdClient.Extend(fd, newsize);
}

int64_t GetFileSize4Nebd(int fd) {
    return nebd::client::nebdClient.GetFileSize(fd);
}

int Discard4Nebd(int fd, NebdClientAioContext* aioctx) {
    return nebd::client::nebdClient.Discard(fd, aioctx);
}

int AioRead4Nebd(int fd, NebdClientAioContext* aioctx) {
    return nebd::client::nebdClient.AioRead(fd, aioctx);
}

int AioWrite4Nebd(int fd, NebdClientAioContext* aioctx) {
    return nebd::client::nebdClient.AioWrite(fd, aioctx);
}

int Flush4Nebd(int fd, NebdClientAioContext* aioctx) {
    return nebd::client::nebdClient.Flush(fd, aioctx);
}

int64_t GetInfo4Nebd(int fd) {
    return nebd::client::nebdClient.GetInfo(fd);
}

int InvalidCache4Nebd(int fd) {
    return nebd::client::nebdClient.InvalidCache(fd);
}
