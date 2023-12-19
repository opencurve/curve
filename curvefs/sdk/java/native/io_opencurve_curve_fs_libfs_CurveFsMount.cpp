/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-08-08
 * Author: Jingli Chen (Wine93)
 */

#include <vector>
#include <utility>
#include <memory>
#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "curvefs/sdk/libcurvefs/libcurvefs.h"
#include "curvefs/sdk/java/native/io_opencurve_curve_fs_libfs_CurveFsMount.h"

const char* statvfs_cls_name =
    "io/opencurve/curve/fs/libfs/CurveFsMount$StatVfs";
const char* stat_cls_name =
    "io/opencurve/curve/fs/libfs/CurveFsMount$Stat";
const char* file_cls_name =
    "io/opencurve/curve/fs/libfs/CurveFsMount$File";
const char* dirent_cls_name =
    "io/opencurve/curve/fs/libfs/CurveFsMount$Dirent";

// Cached for class
static jclass statvfs_cls;
static jclass stat_cls;
static jclass file_cls;
static jclass dirent_cls;

// Cached field IDs for io.opencurve.curve.fs.CurveFsMount.StatVfs
static jfieldID statvfs_bsize_fid;
static jfieldID statvfs_frsize_fid;
static jfieldID statvfs_blocks_fid;
static jfieldID statvfs_bavail_fid;
static jfieldID statvfs_files_fid;
static jfieldID statvfs_fsid_fid;
static jfieldID statvfs_namemax_fid;

// Cached field IDs for io.opencurve.curve.fs.CurveFsMount.Stat
static jfieldID stat_mode_fid;
static jfieldID stat_uid_fid;
static jfieldID stat_gid_fid;
static jfieldID stat_size_fid;
static jfieldID stat_blksize_fid;
static jfieldID stat_blocks_fid;
static jfieldID stat_atime_fid;
static jfieldID stat_mtime_fid;
static jfieldID stat_isFile_fid;
static jfieldID stat_isDirectory_fid;
static jfieldID stat_isSymlink_fid;

// Cached field IDs for io.opencurve.curve.fs.CurveFsMount.File
static jfieldID file_fd_fid;
static jfieldID file_length_fid;

// Cached field IDs for io.opencurve.curve.fs.CurveFsMount.Dirent
static jfieldID dirent_name_fid;
static jfieldID dirent_stat_fid;


// Setup cached field IDs
static void setup_field_ids(JNIEnv* env) {
/*
 * Get a fieldID from a class with a specific type
 *
 * clz: jclass
 * field: field in clz
 * type: integer, long, etc..
 *
 * This macro assumes some naming convention that is used
 * only in this file:
 *
 *   GETFID(curvestat, mode, I) gets translated into
 *     curvestat_mode_fid = env->GetFieldID(curvestat_cls, "mode", "I");
 */
#define GETFID(clz, field, type) do { \
    clz ## _ ## field ## _fid = env->GetFieldID(clz ## _cls, #field, #type); \
    if (!clz ## _ ## field ## _fid) \
        return; \
    } while (0)

    // Cache StatVfs fields
    statvfs_cls = env->FindClass(statvfs_cls_name);
    if (!statvfs_cls) {
        return;
    }

    GETFID(statvfs, bsize, J);
    GETFID(statvfs, frsize, J);
    GETFID(statvfs, blocks, J);
    GETFID(statvfs, bavail, J);
    GETFID(statvfs, files, J);
    GETFID(statvfs, fsid, J);
    GETFID(statvfs, namemax, J);

    // Cache Stat fields
    stat_cls = env->FindClass(stat_cls_name);
    if (!stat_cls) {
        return;
    }

    GETFID(stat, mode, I);
    GETFID(stat, uid, I);
    GETFID(stat, gid, I);
    GETFID(stat, size, J);
    GETFID(stat, blksize, J);
    GETFID(stat, blocks, J);
    GETFID(stat, atime, J);
    GETFID(stat, mtime, J);
    GETFID(stat, isFile, Z);
    GETFID(stat, isDirectory, Z);
    GETFID(stat, isSymlink, Z);

    // Cache File fields
    file_cls = env->FindClass(file_cls_name);
    if (!file_cls) {
        return;
    }

    GETFID(file, fd, I);
    GETFID(file, length, J);

    // Cache Dirent fields
    dirent_cls = env->FindClass(dirent_cls_name);
    if (!dirent_cls) {
        return;
    }

    GETFID(dirent, name, Ljava/lang/String;);
    GETFID(dirent, stat, Lio/opencurve/curve/fs/libfs/CurveFsMount$Stat;);

#undef GETFID
}

static void fill_statvfs(JNIEnv* env, jobject j_statvfs, struct statvfs* st) {
    env->SetLongField(j_statvfs, statvfs_bsize_fid, st->f_bsize);
    env->SetLongField(j_statvfs, statvfs_frsize_fid, st->f_frsize);
    env->SetLongField(j_statvfs, statvfs_blocks_fid, st->f_blocks);
    env->SetLongField(j_statvfs, statvfs_bavail_fid, st->f_bavail);
    env->SetLongField(j_statvfs, statvfs_files_fid, st->f_files);
    env->SetLongField(j_statvfs, statvfs_fsid_fid, st->f_fsid);
    env->SetLongField(j_statvfs, statvfs_namemax_fid, st->f_namemax);
}

static void fill_stat(JNIEnv* env, jobject j_stat, struct stat* stat) {
    env->SetIntField(j_stat, stat_mode_fid, stat->st_mode);
    env->SetIntField(j_stat, stat_uid_fid, stat->st_uid);
    env->SetIntField(j_stat, stat_gid_fid, stat->st_gid);
    env->SetLongField(j_stat, stat_size_fid, stat->st_size);
    env->SetLongField(j_stat, stat_blksize_fid, stat->st_blksize);
    env->SetLongField(j_stat, stat_blocks_fid, stat->st_blocks);

    // mtime
    uint64_t time = stat->st_mtim.tv_sec;
    time *= 1000;
    time += stat->st_mtim.tv_nsec / 1000000;
    env->SetLongField(j_stat, stat_mtime_fid, time);

    // atime
    time = stat->st_atim.tv_sec;
    time *= 1000;
    time += stat->st_atim.tv_nsec / 1000000;
    env->SetLongField(j_stat, stat_atime_fid, time);

    env->SetBooleanField(j_stat, stat_isFile_fid,
        S_ISREG(stat->st_mode) ? JNI_TRUE : JNI_FALSE);

    env->SetBooleanField(j_stat, stat_isDirectory_fid,
        S_ISDIR(stat->st_mode) ? JNI_TRUE : JNI_FALSE);

    env->SetBooleanField(j_stat, stat_isSymlink_fid,
        S_ISLNK(stat->st_mode) ? JNI_TRUE : JNI_FALSE);
}

static void fill_file(JNIEnv* env, jobject j_file, file_t* file) {
    env->SetIntField(j_file, file_fd_fid, file->fd);
    env->SetLongField(j_file, file_length_fid, file->length);
}

static void fill_dirent(JNIEnv* env, jobject j_dirent, dirent_t* dirent) {
    jstring j_name = env->NewStringUTF(dirent->name);
    jobject j_stat = env->AllocObject(env->FindClass(stat_cls_name));
    fill_stat(env, j_stat, &dirent->stat);

    env->SetObjectField(j_dirent, dirent_name_fid, j_name);
    env->SetObjectField(j_dirent, dirent_stat_fid, j_stat);

    env->DeleteLocalRef(j_name);
    env->DeleteLocalRef(j_stat);
}

// Map io_opencurve_curve_fs_libfs_CurveFsMount_O_* open flags to values in libc
static inline uint32_t fixup_open_flags(jint jflags) {
    uint32_t flags = 0;

#define FIXUP_OPEN_FLAG(name) \
    if (jflags & io_opencurve_curve_fs_libfs_CurveFsMount_##name) \
        flags |= name;

    FIXUP_OPEN_FLAG(O_RDONLY)
    FIXUP_OPEN_FLAG(O_RDWR)
    FIXUP_OPEN_FLAG(O_APPEND)
    FIXUP_OPEN_FLAG(O_CREAT)
    FIXUP_OPEN_FLAG(O_TRUNC)
    FIXUP_OPEN_FLAG(O_EXCL)
    FIXUP_OPEN_FLAG(O_WRONLY)
    FIXUP_OPEN_FLAG(O_DIRECTORY)

#undef FIXUP_OPEN_FLAG

    return flags;
}

#define CURVEFS_SETATTR_MODE       (1 << 0)
#define CURVEFS_SETATTR_UID        (1 << 1)
#define CURVEFS_SETATTR_GID        (1 << 2)
#define CURVEFS_SETATTR_SIZE       (1 << 3)
#define CURVEFS_SETATTR_ATIME      (1 << 4)
#define CURVEFS_SETATTR_MTIME      (1 << 5)
#define CURVEFS_SETATTR_ATIME_NOW  (1 << 7)
#define CURVEFS_SETATTR_MTIME_NOW  (1 << 8)
#define CURVEFS_SETATTR_CTIME      (1 << 10)

// Map JAVA_SETATTR_* to values in curve lib
static inline int fixup_attr_mask(jint jmask) {
    int mask = 0;

#define FIXUP_ATTR_MASK(name) \
    if (jmask & io_opencurve_curve_fs_libfs_CurveFsMount_##name) \
        mask |= CURVEFS_##name;

    FIXUP_ATTR_MASK(SETATTR_MODE)
    FIXUP_ATTR_MASK(SETATTR_UID)
    FIXUP_ATTR_MASK(SETATTR_GID)
    FIXUP_ATTR_MASK(SETATTR_MTIME)
    FIXUP_ATTR_MASK(SETATTR_ATIME)

#undef FIXUP_ATTR_MASK
    return mask;
}

// Exception throwing helper. Adapted from Apache Hadoop header
// org_apache_hadoop.h by adding the do {} while (0) construct.
#define THROW(env, exception_name, message) \
    do { \
        jclass ecls = env->FindClass(exception_name); \
        if (ecls) { \
            int ret = env->ThrowNew(ecls, message); \
            if (ret < 0) { \
                printf("(CurveFs) Fatal Error\n"); \
            } \
            env->DeleteLocalRef(ecls); \
        } \
    } while (0)

static void handle_error(JNIEnv* env, int rc) {
    switch (rc) {
        case ENOENT:
            THROW(env, "java/io/FileNotFoundException", "");
            return;
        case EEXIST:
            THROW(env, "org/apache/hadoop/fs/FileAlreadyExistsException", "");
            return;
        case ENOTDIR:
            THROW(env, "io/opencurve/curve/fs/libfs/CurveFsException$NotADirectoryException", "");  // NOLINT
            return;
        default:
            break;
    }

    THROW(env, "java/io/IOException", strerror(rc));
}

// nativeCurveFsCreate: curvefs_new
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsNew
  (JNIEnv* env, jobject) {
    setup_field_ids(env);
    uintptr_t instance = curvefs_new();
    return reinterpret_cast<uint64_t>(instance);
}

// nativeCurveFsRelease: curvefs_delete
JNIEXPORT void
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsDelete
    (JNIEnv* env, jobject, jlong j_instance) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    return curvefs_delete(instance);
}

// nativeCurveFsConfSet: curvefs_conf_set
JNIEXPORT void
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsConfSet
    (JNIEnv* env, jclass, jlong j_instance, jstring j_key, jstring j_value) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* key = env->GetStringUTFChars(j_key, NULL);
    const char* value = env->GetStringUTFChars(j_value, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_key, key);
        env->ReleaseStringUTFChars(j_value, value);
    });

    return curvefs_conf_set(instance, key, value);
}

// nativeCurveFsMount: curvefs_mount
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsMount
    (JNIEnv* env, jclass, jlong j_instance,
     jstring j_fsname, jstring j_mountpoint) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* fsname = env->GetStringUTFChars(j_fsname, NULL);
    const char* mountpoint = env->GetStringUTFChars(j_mountpoint, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_fsname, fsname);
        env->ReleaseStringUTFChars(j_mountpoint, mountpoint);
    });

    int rc = curvefs_mount(instance, fsname, mountpoint);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsUmount: curvefs_umount
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsUmount
    (JNIEnv* env, jclass, jlong j_instance,
     jstring j_fsname, jstring j_mountpoint) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* fsname = env->GetStringUTFChars(j_fsname, NULL);
    const char* mountpoint = env->GetStringUTFChars(j_mountpoint, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_fsname, fsname);
        env->ReleaseStringUTFChars(j_mountpoint, mountpoint);
    });

    int rc = curvefs_umonut(instance, fsname, mountpoint);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsMkDirs: curvefs_mkdirs
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsMkDirs
  (JNIEnv* env, jclass, jlong j_instance, jstring j_path, jint j_mode) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    uint16_t mode = static_cast<uint16_t>(j_mode);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_mkdirs(instance, path, mode);
    if (rc == EEXIST) {
        rc = 0;
    } else if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsRmDir: curvefs_rmdir
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsRmDir
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_rmdir(instance, path);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsListDir: curvefs_opendir/curvefs_readdir/curvefs_closedir
JNIEXPORT jobjectArray
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsListDir
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    // curvefs_opendir
    uint64_t fd;
    auto rc = curvefs_opendir(instance, path, &fd);
    if (rc != 0) {
        handle_error(env, rc);
        return NULL;
    }

    // curvefs_readdir
    std::vector<dirent_t> dirents;
    std::vector<dirent_t> buffer(8192);
    for ( ; ; ) {
        ssize_t n = curvefs_readdir(instance, fd, buffer.data(), 8192);
        if (n < 0) {
            handle_error(env, rc);
            return NULL;
        } else if (n == 0) {
            break;
        }

        // TODO(Wine93): less memory copy
        dirents.insert(dirents.end(), buffer.begin(), buffer.begin() + n);
    }

    // closedir
    rc = curvefs_closedir(instance, fd);
    if (rc != 0) {
        handle_error(env, rc);
        return NULL;
    }

    jobjectArray j_dirents = env->NewObjectArray(dirents.size(),
                                                 env->FindClass(dirent_cls_name),
                                                 NULL);

    for (int i = 0; i < dirents.size(); i++) {
        // NOTE!!!: don't use static class
        jobject j_dirent = env->AllocObject(env->FindClass(dirent_cls_name));
        fill_dirent(env, j_dirent, &dirents[i]);
        env->SetObjectArrayElement(j_dirents, i, j_dirent);
        env->DeleteLocalRef(j_dirent);
    }

    return j_dirents;
}

// nativeCurveFsOpen: curvefs_create
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsCreate
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jint j_mode, jobject j_file) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    uint16_t mode = static_cast<uint16_t>(j_mode);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    file_t file;
    int rc = curvefs_create(instance, path, mode, &file);
    if (rc < 0) {
        handle_error(env, rc);
    }

    fill_file(env, j_file, &file);
    return rc;
}

// nativeCurveFsOpen: curvefs_open
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsOpen
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jint j_flags, jobject j_file) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    uint32_t flags = fixup_open_flags(j_flags);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    file_t file;
    int rc = curvefs_open(instance, path, flags, &file);
    if (rc < 0) {
        handle_error(env, rc);
    }

    fill_file(env, j_file, &file);
    return rc;
}

// nativeCurveFsLSeek: curvefs_lseek
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsLSeek
    (JNIEnv* env, jclass,
     jlong j_instance, jint j_fd, jlong j_offset, jint j_whence) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);
    uint64_t offset = static_cast<uint64_t>(j_offset);

    int whence;
    switch (j_whence) {
    case io_opencurve_curve_fs_libfs_CurveFsMount_SEEK_SET:
        whence = SEEK_SET;
        break;
    case io_opencurve_curve_fs_libfs_CurveFsMount_SEEK_CUR:
        whence = SEEK_CUR;
        break;
    case io_opencurve_curve_fs_libfs_CurveFsMount_SEEK_END:
        whence = SEEK_END;
        break;
    default:
        return -1;
    }

    int rc = curvefs_lseek(instance, fd, offset, whence);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativieCurveFsRead: curvefs_read
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativieCurveFsRead
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd,
     jlong offset, jbyteArray j_buffer, jlong j_size) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);
    jbyte* c_buffer = env->GetByteArrayElements(j_buffer, NULL);
    char* buffer = reinterpret_cast<char*>(c_buffer);
    size_t count = static_cast<size_t>(j_size);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseByteArrayElements(j_buffer, c_buffer, 0);
    });

    ssize_t n = curvefs_read(instance, fd, buffer, count);
    if (n < 0) {
        handle_error(env, n);
    }
    return static_cast<jint>(n);
}

// nativieCurveFsWrite: curvefs_write
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativieCurveFsWrite
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd,
     jlong offset, jbyteArray j_buffer, jlong j_size) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);
    jbyte* c_buffer = env->GetByteArrayElements(j_buffer, NULL);
    char* buffer = reinterpret_cast<char*>(c_buffer);
    size_t count = static_cast<size_t>(j_size);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseByteArrayElements(j_buffer, c_buffer, 0);
    });

    ssize_t n = curvefs_write(instance, fd, buffer, count);
    if (n < 0) {
        handle_error(env, n);
    }
    return static_cast<jint>(n);
}

// nativeCurveFsFSync: curvefs_fsync
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsFSync
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    int rc = curvefs_fsync(instance, fd);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsClose: curvefs_close
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsClose
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    int rc = curvefs_close(instance, fd);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsUnlink: curvefs_unlink
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsUnlink
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_unlink(instance, path);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsStatFs: curvefs_statfs
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsStatFs
    (JNIEnv* env, jclass,
     jlong j_instance, jobject j_statvfs) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);

    struct statvfs statvfs;
    int rc = curvefs_statfs(instance, &statvfs);
    if (rc != 0) {
        handle_error(env, rc);
        return rc;
    }

    fill_statvfs(env, j_statvfs, &statvfs);
    return rc;
}

// nativeCurveFsLstat: curvefs_lstat
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsLStat
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jobject j_stat) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    // curvefs_lstat
    struct stat stat;
    auto rc = curvefs_lstat(instance, path, &stat);
    if (rc != 0) {
        handle_error(env, rc);
        return rc;
    }

    fill_stat(env, j_stat, &stat);
    return rc;
}

// nativeCurveFsFStat: curvefs_fstat
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsFStat
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd, jobject j_stat) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    // curvefs_fstat
    struct stat stat;
    auto rc = curvefs_fstat(instance, fd, &stat);
    if (rc != 0) {
        handle_error(env, rc);
        return rc;
    }

    fill_stat(env, j_stat, &stat);
    return rc;
}

// nativeCurveFsSetAttr: curvefs_setattr
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsSetAttr
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jobject j_stat, jint j_mask) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    int to_set = fixup_attr_mask(j_mask);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    struct stat stat;
    memset(&stat, 0, sizeof(stat));
    stat.st_mode = env->GetIntField(j_stat, stat_mode_fid);
    stat.st_uid = env->GetIntField(j_stat, stat_uid_fid);
    stat.st_gid = env->GetIntField(j_stat, stat_gid_fid);
    uint64_t mtime_msec = env->GetLongField(j_stat, stat_mtime_fid);
    uint64_t atime_msec = env->GetLongField(j_stat, stat_atime_fid);
    stat.st_mtim.tv_sec = mtime_msec / 1000;
    stat.st_mtim.tv_nsec = (mtime_msec % 1000) * 1000000;
    stat.st_atim.tv_sec = atime_msec / 1000;
    stat.st_atim.tv_nsec = (atime_msec % 1000) * 1000000;

    int rc = curvefs_setattr(instance, path, &stat, to_set);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsChmod: curvefs_chmod
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsChmod
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path, jint j_mode) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    uint16_t mode = static_cast<uint16_t>(j_mode);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_chmod(instance, path, mode);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsChown: curvefs_chown
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsChown
  (JNIEnv* env, jclass,
   jlong j_instance, jstring j_path, jint j_uid, jint j_gid) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    uint32_t uid = static_cast<uint32_t>(j_uid);
    uint32_t gid = static_cast<uint32_t>(j_gid);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_chown(instance, path, uid, gid);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsRename: curvefs_rename
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsRename
    (JNIEnv* env, jclass, jlong j_instance, jstring j_src, jstring j_dst) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* src = env->GetStringUTFChars(j_src, NULL);
    const char* dst = env->GetStringUTFChars(j_dst, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_src, src);
        env->ReleaseStringUTFChars(j_dst, dst);
    });

    int rc = curvefs_rename(instance, src, dst);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsRemove: curvefs_remove
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsRemove
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_remove(instance, path);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFsRemoveAll: curvefs_removeall
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFsMount_nativeCurveFsRemoveAll
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int rc = curvefs_removeall(instance, path);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}
