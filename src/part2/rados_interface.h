/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_RADOS_INTERFACE_H_
#define SRC_PART2_RADOS_INTERFACE_H_

#include <rbd/librbd.h>
#include <rados/librados.h>
#include "src/part2/common_type.h"

int OpenImage(rados_t* cluster, const char* poolname, const char* volname,
               int fd, char* filename);
rados_t* ConnectRados(const char* mon_host);
void CloseRados(rados_t* cluster);
int FilenameFdExist(char* filename);
int CloseImage(int fd);
bool FdExist(int fd, rbd_image_t** image);

#endif  // SRC_PART2_RADOS_INTERFACE_H_
