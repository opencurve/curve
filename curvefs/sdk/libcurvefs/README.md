libcurvefs
===

SDK C/C++ library for CurveFS.

Example
===

```c
#include "libcurvefs.h"

int instance = curvefs_create();
curvefs_conf_set(instance, "s3.ak", "xxx")
curvefs_conf_set(instance, "s3.sk", "xxx")

...

int rc = curvefs_mount(instance, "fsname", "/);
if (rc != 0) {
    // mount failed
}

rc = curvefs_mkdir(instance_ptr, "/mydir")
if (rc != 0) {
    // mkdir failed
}
```
