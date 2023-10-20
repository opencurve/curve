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

#ifndef CURVEFS_SDK_LIBCURVEFS_EXAMPLES_COMMON_H_
#define CURVEFS_SDK_LIBCURVEFS_EXAMPLES_COMMON_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "curvefs/sdk/libcurvefs/libcurvefs.h"

const char* KEY_FSNAME = "CURVEFS_FSNAME";
const char* KEY_S3_AK = "s3.ak";
const char* KEY_S3_SK = "s3.sk";
const char* KEY_S3_ENDPOINT = "s3.endpoint";
const char* KEY_S3_BUCKET = "s3.bucket_name";
const char* KEY_MDS_ADDRS = "mdsOpt.rpcRetryOpt.addrs";

char*
require_string(const char* name) {
    char* value = getenv(name);
    if (strlen(value) == 0) {
        fprintf(stderr, "require %s\n", name);
        exit(1);
    }
    return value;
}

char*
get_filesystem_name() {
    return require_string(KEY_FSNAME);
}

char*
get_mountpoint() {
    return "/";
}

void
load_cfg_from_environ(uintptr_t instance) {
    curvefs_conf_set(instance, KEY_S3_AK, require_string(KEY_S3_AK));
    curvefs_conf_set(instance, KEY_S3_SK, require_string(KEY_S3_SK));
    curvefs_conf_set(instance, KEY_S3_ENDPOINT,
                     require_string(KEY_S3_ENDPOINT));
    curvefs_conf_set(instance, KEY_S3_BUCKET, require_string(KEY_S3_BUCKET));
    curvefs_conf_set(instance, KEY_MDS_ADDRS, require_string(KEY_MDS_ADDRS));
    curvefs_conf_set(instance, "fs.accessLogging", "true");
    curvefs_conf_set(instance, "client.loglevel", "6");
    curvefs_conf_set(instance, "diskCache.diskCacheType", "0");
}

void
exact_args(int argc, int number) {
    if (--argc == number) {
        return;
    }

    fprintf(stderr, "requires exactly %d argument[s]\n", number);
    exit(1);
}

#endif  // CURVEFS_SDK_LIBCURVEFS_EXAMPLES_COMMON_H_
