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

#include "absl/cleanup/cleanup.h"
#include "curvefs/sdk/libcurvefs/libcurvefs.h"
#include "curvefs/sdk/java/native/io_opencurve_curve_fs_libfs_CurveFSMount.h"

/* Cached field IDs for io.opencurve.curve.fs.CurveStat */
static jfieldID curvestat_mode_fid;
static jfieldID curvestat_uid_fid;
static jfieldID curvestat_gid_fid;
static jfieldID curvestat_size_fid;
static jfieldID curvestat_blksize_fid;
static jfieldID curvestat_blocks_fid;
static jfieldID curvestat_a_time_fid;
static jfieldID curvestat_m_time_fid;
static jfieldID curvestat_is_file_fid;
static jfieldID curvestat_is_directory_fid;
static jfieldID curvestat_is_symlink_fid;

/* Cached field IDs for io.opencurve.curve.fs.CurveStatVFS */
static jfieldID curvestatvfs_bsize_fid;
static jfieldID curvestatvfs_frsize_fid;
static jfieldID curvestatvfs_blocks_fid;
static jfieldID curvestatvfs_bavail_fid;
static jfieldID curvestatvfs_files_fid;
static jfieldID curvestatvfs_fsid_fid;
static jfieldID curvestatvfs_namemax_fid;

/*
 * Setup cached field IDs
 */
static void setup_field_ids(JNIEnv* env) {
    jclass curvestat_cls;
    jclass curvestatvfs_cls;

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

    /* Cache CurveStat fields */

    curvestat_cls = env->FindClass("io/opencurve/curve/fs/libfs/CurveFSStat");
    if (!curvestat_cls) {
        return;
    }

    GETFID(curvestat, mode, I);
    GETFID(curvestat, uid, I);
    GETFID(curvestat, gid, I);
    GETFID(curvestat, size, J);
    GETFID(curvestat, blksize, J);
    GETFID(curvestat, blocks, J);
    GETFID(curvestat, a_time, J);
    GETFID(curvestat, m_time, J);
    GETFID(curvestat, is_file, Z);
    GETFID(curvestat, is_directory, Z);
    GETFID(curvestat, is_symlink, Z);

    /* Cache CurveStatVFS fields */

    curvestatvfs_cls =
        env->FindClass("io/opencurve/curve/fs/libfs/CurveFSStatVFS");
    if (!curvestatvfs_cls) {
        return;
    }

    GETFID(curvestatvfs, bsize, J);
    GETFID(curvestatvfs, frsize, J);
    GETFID(curvestatvfs, blocks, J);
    GETFID(curvestatvfs, bavail, J);
    GETFID(curvestatvfs, files, J);
    GETFID(curvestatvfs, fsid, J);
    GETFID(curvestatvfs, namemax, J);

#undef GETFID
}

static void fill_curvestat(JNIEnv* env,
                           jobject j_curvestat,
                           struct stat* stat) {
    env->SetIntField(j_curvestat, curvestat_mode_fid, stat->st_mode);
    env->SetIntField(j_curvestat, curvestat_uid_fid, stat->st_uid);
    env->SetIntField(j_curvestat, curvestat_gid_fid, stat->st_gid);
    env->SetLongField(j_curvestat, curvestat_size_fid, stat->st_size);
    env->SetLongField(j_curvestat, curvestat_blksize_fid, stat->st_blksize);
    env->SetLongField(j_curvestat, curvestat_blocks_fid, stat->st_blocks);

    // mtime
    uint64_t time = stat->st_mtim.tv_sec;
    time *= 1000;
    time += stat->st_mtim.tv_nsec / 1000000;
    env->SetLongField(j_curvestat, curvestat_m_time_fid, time);

    // atime
    time = stat->st_atim.tv_sec;
    time *= 1000;
    time += stat->st_atim.tv_nsec / 1000000;
    env->SetLongField(j_curvestat, curvestat_a_time_fid, time);

    env->SetBooleanField(j_curvestat, curvestat_is_file_fid,
        S_ISREG(stat->st_mode) ? JNI_TRUE : JNI_FALSE);

    env->SetBooleanField(j_curvestat, curvestat_is_directory_fid,
        S_ISDIR(stat->st_mode) ? JNI_TRUE : JNI_FALSE);

    env->SetBooleanField(j_curvestat, curvestat_is_symlink_fid,
        S_ISLNK(stat->st_mode) ? JNI_TRUE : JNI_FALSE);
}

static void fill_curvestatvfs(JNIEnv* env,
                              jobject j_curvestatvfs,
                              struct statvfs st) {
    env->SetLongField(j_curvestatvfs, curvestatvfs_bsize_fid, st.f_bsize);
    env->SetLongField(j_curvestatvfs, curvestatvfs_frsize_fid, st.f_frsize);
    env->SetLongField(j_curvestatvfs, curvestatvfs_blocks_fid, st.f_blocks);
    env->SetLongField(j_curvestatvfs, curvestatvfs_bavail_fid, st.f_bavail);
    env->SetLongField(j_curvestatvfs, curvestatvfs_files_fid, st.f_files);
    env->SetLongField(j_curvestatvfs, curvestatvfs_fsid_fid, st.f_fsid);
    env->SetLongField(j_curvestatvfs, curvestatvfs_namemax_fid, st.f_namemax);
}

/* Map io_opencurve_curve_fs_libfs_CurveFSMount_O_* open flags to values in libc */
static inline uint32_t fixup_open_flags(jint jflags) {
    uint32_t flags = 0;

#define FIXUP_OPEN_FLAG(name) \
    if (jflags & io_opencurve_curve_fs_libfs_CurveFSMount_##name) \
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

/* Map JAVA_SETATTR_* to values in curve lib */
static inline int fixup_attr_mask(jint jmask) {
    int mask = 0;

#define FIXUP_ATTR_MASK(name) \
    if (jmask & io_opencurve_curve_fs_libfs_CurveFSMount_##name) \
        mask |= CURVEFS_##name;

    FIXUP_ATTR_MASK(SETATTR_MODE)
    FIXUP_ATTR_MASK(SETATTR_UID)
    FIXUP_ATTR_MASK(SETATTR_GID)
    FIXUP_ATTR_MASK(SETATTR_MTIME)
    FIXUP_ATTR_MASK(SETATTR_ATIME)

#undef FIXUP_ATTR_MASK
    return mask;
}

/*
 * Exception throwing helper. Adapted from Apache Hadoop header
 * org_apache_hadoop.h by adding the do {} while (0) construct.
 */
#define THROW(env, exception_name, message) \
    do { \
        jclass ecls = env->FindClass(exception_name); \
        if (ecls) { \
            int ret = env->ThrowNew(ecls, message); \
            if (ret < 0) { \
                printf("(CurveFS) Fatal Error\n"); \
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
            THROW(env, "org/apache/hadoop/fs/ParentNotDirectoryException", "");
            return;
        default:
            break;
    }

    THROW(env, "java/io/IOException", strerror(rc));
}

// nativeCurveFSCreate: curvefs_create
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSCreate
    (JNIEnv* env, jobject) {
    setup_field_ids(env);
    uintptr_t instance = curvefs_create();
    return reinterpret_cast<uint64_t>(instance);
}

// nativeCurveFSRelease: curvefs_release
JNIEXPORT void
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSRelease
    (JNIEnv* env, jobject, jlong j_instance) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    return curvefs_release(instance);
}

// nativeCurveFSConfSet: curvefs_conf_set
JNIEXPORT void
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSConfSet
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

// nativeCurveFSMount: curvefs_mount
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSMount
    (JNIEnv* env, jclass, jlong j_instance, jstring j_fsname) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* fsname = env->GetStringUTFChars(j_fsname, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_fsname, fsname);
    });

    return curvefs_mount(instance, fsname, "/");
}

// nativeCurveFSUmount: curvefs_umount
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSUmount
    (JNIEnv* env, jclass, jlong j_instance) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    return curvefs_umonut(instance);
}

// nativeCurveFSMkDirs: curvefs_mkdirs
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSMkDirs
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

// nativeCurveFSRmDir: curvefs_rmdir
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSRmDir
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

// nativeCurveFSListDir: curvefs_opendir/curvefs_readdir/curvefs_closedir
JNIEXPORT jobjectArray
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSListDir
    (JNIEnv* env, jclass, jlong j_instance, jstring j_path) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    // curvefs_opendir
    dir_stream_t dir_stream;
    auto rc = curvefs_opendir(instance, path, &dir_stream);
    if (rc != 0) {
        handle_error(env, rc);
        return NULL;
    }

    // curvefs_readdir
    std::vector<dirent_t> dirents;
    dirent_t dirent;
    for ( ;; ) {
        ssize_t n = curvefs_readdir(instance, &dir_stream, &dirent);
        if (n < 0) {
            handle_error(env, rc);
            return NULL;
        } else if (n == 0) {
            break;
        }
        dirents.push_back(dirent);
    }

    // closedir
    rc = curvefs_closedir(instance, &dir_stream);
    if (rc != 0) {
        handle_error(env, rc);
        return NULL;
    }

    // extrace entry name
    jobjectArray j_names = env->NewObjectArray(
        dirents.size(), env->FindClass("java/lang/String"), NULL);

    for (int i = 0; i < dirents.size(); i++) {
        jstring j_name = env->NewStringUTF(dirents[i].name);
        env->SetObjectArrayElement(j_names, i, j_name);
        env->DeleteLocalRef(j_name);
    }
    return j_names;
}

// nativeCurveFSOpen: curvefs_open
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSOpen
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jint j_flags, jint j_mode) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    uint32_t flags = fixup_open_flags(j_flags);
    uint16_t mode = static_cast<uint16_t>(j_mode);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    int fd = curvefs_open(instance, path, flags, mode);
    if (fd < 0) {
        handle_error(env, fd);
    }
    return fd;
}

// nativeCurveFSLSeek: curvefs_lseek
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSLSeek
    (JNIEnv* env, jclass,
     jlong j_instance, jint j_fd, jlong j_offset, jint j_whence) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);
    uint64_t offset = static_cast<uint64_t>(j_offset);

    int whence;
    switch (j_whence) {
    case io_opencurve_curve_fs_libfs_CurveFSMount_SEEK_SET:
        whence = SEEK_SET;
        break;
    case io_opencurve_curve_fs_libfs_CurveFSMount_SEEK_CUR:
        whence = SEEK_CUR;
        break;
    case io_opencurve_curve_fs_libfs_CurveFSMount_SEEK_END:
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

// nativieCurveFSRead: curvefs_read
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativieCurveFSRead
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd,
     jbyteArray j_buffer, jlong j_size, jlong j_offset) {
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
    return static_cast<jlong>(n);
}

// nativieCurveFSWrite: curvefs_write
JNIEXPORT jlong
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativieCurveFSWrite
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd,
     jbyteArray j_buffer, jlong j_size, jlong j_offset) {
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
    return static_cast<jlong>(n);
}

// nativeCurveFSFSync: curvefs_fsync
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSFSync
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    int rc = curvefs_fsync(instance, fd);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFSClose: curvefs_close
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSClose
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    int rc = curvefs_close(instance, fd);
    if (rc != 0) {
        handle_error(env, rc);
    }
    return rc;
}

// nativeCurveFSUnlink: curvefs_unlink
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSUnlink
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

// nativeCurveFSStatFs: curvefs_statfs
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSStatFs
    (JNIEnv* env, jclass,
     jlong j_instance, jobject j_curvestatvfs) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);

    struct statvfs statvfs;
    int rc = curvefs_statfs(instance, &statvfs);
    if (rc != 0) {
        handle_error(env, rc);
        return rc;
    }

    fill_curvestatvfs(env, j_curvestatvfs, statvfs);
    return rc;
}

// nativeCurveFSLstat: curvefs_lstat
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSLstat
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jobject j_curvestat) {
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

    fill_curvestat(env, j_curvestat, &stat);
    return rc;
}

// nativeCurveFSFStat: curvefs_fstat
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSFStat
    (JNIEnv* env, jclass, jlong j_instance, jint j_fd, jobject j_curvestat) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    int fd = static_cast<int>(j_fd);

    // curvefs_fstat
    struct stat stat;
    auto rc = curvefs_fstat(instance, fd, &stat);
    if (rc != 0) {
        handle_error(env, rc);
        return rc;
    }

    fill_curvestat(env, j_curvestat, &stat);
    return rc;
}

// nativeCurveFSSetAttr: curvefs_setattr
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSSetAttr
    (JNIEnv* env, jclass,
     jlong j_instance, jstring j_path, jobject j_curvestat, jint j_mask) {
    uintptr_t instance = static_cast<uintptr_t>(j_instance);
    const char* path = env->GetStringUTFChars(j_path, NULL);
    int to_set = fixup_attr_mask(j_mask);
    auto defer = absl::MakeCleanup([&]() {
        env->ReleaseStringUTFChars(j_path, path);
    });

    struct stat stat;
    memset(&stat, 0, sizeof(stat));
    stat.st_mode = env->GetIntField(j_curvestat, curvestat_mode_fid);
    stat.st_uid = env->GetIntField(j_curvestat, curvestat_uid_fid);
    stat.st_gid = env->GetIntField(j_curvestat, curvestat_gid_fid);
    uint64_t mtime_msec = env->GetLongField(j_curvestat, curvestat_m_time_fid);
    uint64_t atime_msec = env->GetLongField(j_curvestat, curvestat_a_time_fid);
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

// nativeCurveFSChmod: curvefs_chmod
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSChmod
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

// nativeCurveFSChown: curvefs_chown
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSChown
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

// nativeCurveFSRename: curvefs_rename
JNIEXPORT jint
JNICALL Java_io_opencurve_curve_fs_libfs_CurveFSMount_nativeCurveFSRename
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
