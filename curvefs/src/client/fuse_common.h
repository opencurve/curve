/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_FUSE_COMMON_H_
#define CURVEFS_SRC_CLIENT_FUSE_COMMON_H_

#define FUSE_USE_VERSION 34

#include <stddef.h>
#include <fuse3/fuse_lowlevel.h>

#ifdef __cplusplus
extern "C" {
#endif

struct MountOption {
    char* mountPoint;
    char* fsName;
    char* fsType;
    char* conf;
    char* mdsAddr;
};

static const struct fuse_opt mount_opts[] = {
    { "fsname=%s",
      offsetof(struct MountOption, fsName), 0},

    { "fstype=%s",
      offsetof(struct MountOption, fsType), 0},

    { "conf=%s",
      offsetof(struct MountOption, conf), 0},

    FUSE_OPT_END
};

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CURVEFS_SRC_CLIENT_FUSE_COMMON_H_
