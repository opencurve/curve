#include <sys/statvfs.h>

#include "common.h"

int
main(int argc, char** argv) {
    exact_args(argc, 0);

    uintptr_t instance = curvefs_create();
    load_cfg_from_environ(instance);

    char* fsname = get_filesystem_name();
    char* mountpoint = get_mountpoint();
    int rc = curvefs_mount(instance, fsname, mountpoint);
    if (rc != 0) {
        fprintf(stderr, "mount failed: retcode = %d\n", rc);
        return rc;
    }

    struct statvfs statvfs;
    rc = curvefs_statfs(instance, &statvfs);
    if (rc != 0) {
        fprintf(stderr, "statvfs failed: retcode = %d\n", rc);
        return rc;
    }

    printf("fsid = %d\n", statvfs.f_fsid);
    return 0;
}
