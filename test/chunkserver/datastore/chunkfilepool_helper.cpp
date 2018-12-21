/*
 * Project: curve
 * File Created: Tuesday, 29th January 2019 12:36:29 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include "test/chunkserver/datastore/chunkfilepool_helper.h"

void allocateChunk(std::shared_ptr<LocalFileSystem> fsptr, uint32_t num) {
    char* data = new (std::nothrow) char[16 * 1024 * 1024 + 4096];              // NOLINT
    memset(data, '0', 16 * 1024 * 1024 + 4096);

    fsptr->Mkdir("./chunkfilepool");
    uint32_t count = 0;
    while (count < num) {
        count++;
        auto filename = std::to_string(count);
        std::string tmpchunkfilepath = "./chunkfilepool/" + filename;            // NOLINT

        int ret = fsptr->Open(tmpchunkfilepath.c_str(), O_RDWR | O_CREAT);                  //NOLINT
        if (ret < 0) {
            LOG(ERROR) << "file open failed, " << tmpchunkfilepath.c_str();
            break;
        }
        int fd = ret;

        ret = fsptr->Fallocate(fd, 0, 0, 16 * 1024 * 1024 + 4096);              //NOLINT
        if (ret < 0) {
            fsptr->Close(fd);
            LOG(ERROR) << "Fallocate failed, " << tmpchunkfilepath.c_str();
            break;
        }

        ret = fsptr->Write(fd, data, 0, 16 * 1024 * 1024 + 4096);               //NOLINT
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
