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
 * Project: curve
 * File Created: Tuesday, 29th January 2019 12:36:29 pm
 * Author: tongguangxun
 */

#include "test/chunkserver/datastore/filepool_helper.h"

void allocateChunk(std::shared_ptr<LocalFileSystem> fsptr,
                   uint32_t num,
                   std::string poolDir,
                   uint32_t chunkSize) {
    char* data = new (std::nothrow) char[chunkSize + 4096];              // NOLINT
    memset(data, '0', chunkSize + 4096);

    fsptr->Mkdir(poolDir);
    uint32_t count = 0;
    while (count < num) {
        count++;
        auto filename = std::to_string(count);
        std::string tmpchunkfilepath = poolDir + "/" + filename;            // NOLINT

        int ret = fsptr->Open(tmpchunkfilepath.c_str(), O_RDWR | O_CREAT);                  //NOLINT
        if (ret < 0) {
            LOG(ERROR) << "file open failed, " << tmpchunkfilepath.c_str();
            break;
        }
        int fd = ret;

        ret = fsptr->Fallocate(fd, 0, 0, chunkSize + 4096);              //NOLINT
        if (ret < 0) {
            fsptr->Close(fd);
            LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath.c_str();
            break;
        }

        ret = fsptr->Write(fd, data, 0, chunkSize + 4096);               //NOLINT
        if (ret < 0) {
            fsptr->Close(fd);
            LOG(ERROR) << "write failed, " << tmpchunkfilepath.c_str();
            break;
        }

        ret = fsptr->Fsync(fd);
        if (ret < 0) {
            fsptr->Close(fd);
            LOG(ERROR) << "fsync failed, " << tmpchunkfilepath.c_str();
            break;
        }

        fsptr->Close(fd);
        if (ret < 0) {
            LOG(ERROR) << "close failed, " << tmpchunkfilepath.c_str();
            break;
        }
    }
    delete[] data;
}
