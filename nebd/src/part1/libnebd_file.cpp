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
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 */

#include "nebd/src/part1/libnebd_file.h"
#include <string>
#include "nebd/src/common/configuration.h"
#include "nebd/src/part1/nebd_client.h"


int Init4Nebd(const char* confpath) {
    return nebd::client::nebdClient.Init(confpath);
}

void Uninit4Nebd() {
    nebd::client::nebdClient.Uninit();
    return;
}

int Open4Nebd(const char* filename, const NebdOpenFlags* openflags) {
    return nebd::client::nebdClient.Open(filename, openflags);
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
