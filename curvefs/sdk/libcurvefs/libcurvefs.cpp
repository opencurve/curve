
#include <string>
#include <memory>

#include "curvefs/src/client/vfs/vfs.h"


using ::curvefs::client::vfs::VFS;

struct curvefs_mount_point {
    curvefs_mount_point() {
        vfs = new VFS();

    }

    VFS* vfs() {
        return vfs;
    }

    VFS* vfs;
};

extern "C" int curvefs_create(struct curvefs_mount_point** mount_point) {
    *mount_point = new curvefs_mount_point();
    return 0;
}

extern "C" int curvefs_mount(struct curvefs_mount_point* mount_point) {
    return mount_point->vfs()->mount();  // TODO
}

extern "C" int curvefs_mkdir(struct curvefs_mount_point* mount_point,
                             const char* path,
                             mode_t mode) {
    return mount_point->vfs()->mkdir(path, mode);
}

extern "C" int curvefs_rmdir(struct curvefs_mount_point* mount_point,
                             const char* path) {
    return mount_point->vfs()->rmdir(path);
}
