/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-07-12
 * Author: Jingli Chen (Wine93)
 */


#ifndef LIBCURVEFS_H
#define LIBCURVEFS_H

#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

int curvefs_create(struct curvefs_mount_point** mount_point);
int curvefs_mount(struct curvefs_mount_point* mount_point);
int curvefs_mkdir(struct curvefs_mount_point* mount_point,
                  const char* path, mode_t mode);
int curvefs_rmdir(struct curvefs_mount_point* mount_point, const char* path);

#ifdef __cplusplus
}
#endif

#endif  // LIBCURVEFS_H
