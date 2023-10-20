#include <fcntl.h>

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

    int fd = curvefs_open(instance, argv[1], O_WRONLY, 0777);
    if (fd != 0) {
        rc = fd;
        fprintf(stderr, "open failed: retcode = %d\n", rc);
        return rc;
    }

    ssize_t n = curvefs_write(instance, fd, argv[2], strlen(argv[2]));
    if (n < 0) {
        rc = n;
        fprintf(stderr, "write failed: retcode = %d\n", rc);
        return rc;
    } else if (n != strlen(argv[2])) {
        fprintf(stderr, "write failed: %zd != %zu\n", n, strlen(argv[2]));
        return -1;
    }

    rc = curvefs_close(instance, fd);
    if (rc != 0) {
        fprintf(stderr, "close failed: retcode = %d\n", rc);
    }
    return 0;
}
