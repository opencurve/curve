#include "common.h"

int
main(int argc, char** argv) {
    exact_args(argc, 1);

    uintptr_t instance = curvefs_create();
    load_cfg_from_environ(instance);

    char* fsname = get_filesystem_name();
    char* mountpoint = get_mountpoint();
    int rc = curvefs_mount(instance, fsname, mountpoint);
    if (rc != 0) {
        fprintf(stderr, "mount failed: retcode = %d\n", rc);
        return rc;
    }

    rc = curvefs_rmdir(instance, argv[1]);
    if (rc != 0) {
        fprintf(stderr, "rmdir failed: retcode = %d\n", rc);
        return rc;
    }

    return 0;
}
