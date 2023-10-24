#include <fcntl.h>

#include "common.h"

#define MAX_BUFFER_SIZE 4096 + 5

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

    int fd = curvefs_open(instance, argv[1], O_WRONLY, 0777);
    if (fd != 0) {
        rc = fd;
        fprintf(stderr, "open failed: retcode = %d\n", rc);
        return rc;
    }

    char buffer[MAX_BUFFER_SIZE];
    for ( ;; ) {
        ssize_t n = curvefs_read(instance, fd, buffer, sizeof(buffer));
        if (n < 0) {
            rc = n;
            fprintf(stderr, "read failed: retcode = %d\n", rc);
            return rc;
        } else if (n == 0) {
            break;
        }

        buffer[n] = '\0';
        printf("%s", buffer);
    }
    return 0;
}
