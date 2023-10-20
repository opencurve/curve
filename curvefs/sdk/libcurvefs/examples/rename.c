#include "common.h"

int
main(int argc, char** argv) {
    exact_args(argc, 2);

    uintptr_t instance = curvefs_create();
    load_cfg_from_environ(instance);

    char* fsname = get_filesystem_name();
    char* mountpoint = get_mountpoint();
    int rc = curvefs_mount(instance, fsname, mountpoint);
    if (rc != 0) {
        fprintf(stderr, "mount failed: retcode = %d\n", rc);
        return rc;
    }

    rc = curvefs_rename(instance, argv[1], argv[2]);
    if (rc != 0) {
        fprintf(stderr, "rename failed: retcode = %d\n", rc);
        return rc;
    }
    return 0;
}
