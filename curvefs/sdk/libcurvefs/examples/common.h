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
    return "test-001";
}

static char *rand_string(char *str, size_t size)
{
    srand(time(NULL));
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK...";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

char* rand_string_alloc(size_t size)
{
     char *s = malloc(size + 1);
     if (s) {
         rand_string(s, size);
     }
     return s;
}

char* get_mountpoint() {
    return rand_string_alloc(20);
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
    curvefs_conf_set(instance, "s3.ak", "curve");
    curvefs_conf_set(instance, "s3.sk", "xxxx");
    curvefs_conf_set(instance, "s3.endpoint", "10.246.159.29:9000");
    curvefs_conf_set(instance, "s3.bucket_name", "hadoop-fs");
    curvefs_conf_set(instance, "mdsOpt.rpcRetryOpt.addrs",
                     "10.246.159.29:6700,10.246.159.31:6700,10.246.159.75:6700");
    curvefs_conf_set(instance, "fs.accessLogging", "true");
    curvefs_conf_set(instance, "client.loglevel", "6");
}

void
exact_args(int argc, int number) {
    if (--argc == number) {
        return;
    }

    fprintf(stderr, "requires exactly %d argument[s]\n", number);
    exit(1);
}
