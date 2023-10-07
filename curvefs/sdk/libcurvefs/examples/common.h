#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "curvefs/sdk/libcurvefs/libcurvefs.h"

const char* KEY_FSNAME = "CURVEFS_FSNAME";

char*
require_string(const char* name) {
    char* value = getenv(name);
    if (strlen(value) == 0) {
        fprintf(stderr, "require %s\n", name);
        exit(1);
    }
    return value;
}

char* get_filesystem_name() {
    //return require_string(KEY_FSNAME);
    return "hadoop-001";
}


char* get_mountpoint() {
    return "/";
}

/*
s3.ak: curve
s3.sk: xxxx
s3.endpoint: 10.246.159.29:9000
s3.bucket_name: hadoop-fs
mdsOpt.rpcRetryOpt.addrs: 10.246.159.29:6700,10.246.159.31:6700,10.246.159.75:6700
*/
void
load_cfg_from_environ(uintptr_t instance) {
    curvefs_conf_set(instance, "s3.ak", "xxx");
    curvefs_conf_set(instance, "s3.sk", "xxx");
    curvefs_conf_set(instance, "s3.endpoint", "10.221.103.160:19000");
    curvefs_conf_set(instance, "s3.bucket_name", "hadoop");
    curvefs_conf_set(instance, "mdsOpt.rpcRetryOpt.addrs",
                     "10.221.103.160:6700,10.221.103.160:6701,10.221.103.160:6702");
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
