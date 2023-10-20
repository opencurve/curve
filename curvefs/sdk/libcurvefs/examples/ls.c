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
    dir_stream_t dir_stream;
    rc = curvefs_opendir(instance, argv[1], &dir_stream);
    if (rc != 0) {
        fprintf(stderr, "opendir failed: retcode = %d\n", rc);
        return rc;
    }

    // readdir
    dirent_t dirent;
    for ( ;; ) {
        ssize_t n = curvefs_readdir(instance, &dir_stream, &dirent);
        if (n < 0) {
            rc = n;
            fprintf(stderr, "readdir failed: retcode = %d\n", rc);
            break;
        } else if (n == 0) {
            break;
        }

        printf("%s: ino=%d size=%d\n", dirent.name,
                                       dirent.stat.st_ino,
                                       dirent.stat.st_size);
    }

    rc = curvefs_closedir(instance, &dir_stream);
    if (rc != 0) {
        fprintf(stderr, "closedir failed: retcode = %d\n", rc);
    }
    return rc;
}
