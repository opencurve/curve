#include <stdio.h>

#include "curvefs/sdk/libcurvefs/libcurvefs.h"

int main(int argc, char** argv) {
    uintptr_t instance = curvefs_create();
    int rc = curvefs_mount(instance, argv[1], "/")
    if (rc != 0) {
        printf ("mount failed: retcode = %d\n", rc);
        return rc;
    }

    rc = curvefs_mkdir(instance, "/dir1", 0755);
    if (rc != 0) {
        printf ("mkdir failed: retcode = %d\n", rc);
        return rc;
    }

    return 0;
}
