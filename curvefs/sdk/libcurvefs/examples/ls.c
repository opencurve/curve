#include "common.h"

int
main(int argc, char** argv) {
    exact_args(argc, 1);

    uintptr_t instance = curvefs_create();
    load_cfg_from_environ(instance);

    // mount
    char* fsname = get_filesystem_name();
    char* mountpoint = get_mountpoint();
    int rc = curvefs_mount(instance, fsname, mountpoint);
    if (rc != 0) {
        fprintf(stderr, "mount failed: retcode = %d\n", rc);
        return rc;
    }

    // opendir
    uint64_t fd;
    rc = curvefs_opendir(instance, argv[1], &fd);
    if (rc != 0) {
        fprintf(stderr, "opendir failed: retcode = %d\n", rc);
        return rc;
    }

    // readdir
    dirent_t dirents[8192];
    for ( ;; ) {
        ssize_t n = curvefs_readdir(instance, fd, dirents, 8192);
        if (n < 0) {
            rc = n;
            fprintf(stderr, "readdir failed: retcode = %d\n", rc);
            break;
        } else if (n == 0) {
            break;
        }

        for (int i = 0; i < n; i++) {
            printf("%s: ino=%d size=%d\n", dirents[i].name,
                                           dirents[i].stat.st_ino,
                                           dirents[i].stat.st_size);
        }
    }

    rc = curvefs_closedir(instance, fd);
    if (rc != 0) {
        fprintf(stderr, "closedir failed: retcode = %d\n", rc);
    }
    return rc;
}
