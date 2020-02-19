workspace(name = "nebd")

# proto_library, cc_proto_library, and java_proto_library rules implicitly
# depend on @com_google_protobuf for protoc and proto runtimes.
# This statement defines the @com_google_protobuf repo.
http_archive(
    name = "com_google_protobuf",
    sha256 = "cef7f1b5a7c5fba672bec2a319246e8feba471f04dcebfe362d55930ee7c1c30",
    strip_prefix = "protobuf-3.5.0",
    urls = ["https://github.com/google/protobuf/archive/v3.5.0.zip"],
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

#import the  gtest files.
new_git_repository(
    name = "com_google_googletest",
    build_file = "bazel/gmock.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

#Import the glog files.
# brpc内BUILD文件在依赖glog时, 直接指定的依赖是"@com_github_google_glog//:glog"
git_repository(
    name = "com_github_google_glog",
    remote = "http://gerrit.storage.netease.com/curve/curve-glog",
    commit = "fa4439bd6a4aae0486fe1b4e9ba7f2b761976ac2",
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog"
)

# glog depends on gflags-2.2.2
http_archive(
    name = "com_github_gflags_gflags",
    strip_prefix = "gflags-2.2.2",
    urls = [
        "https://mirror.bazel.build/github.com/gflags/gflags/archive/v2.2.2.tar.gz",
        "https://github.com/gflags/gflags/archive/v2.2.2.tar.gz",
    ],
)

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

new_http_archive(
    name = "com_github_google_leveldb",
    build_file = "bazel/leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    url = "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz",
)

bind(
    name = "leveldb",
    actual = "@com_github_google_leveldb//:leveldb",
)

git_repository(
    name = "com_netease_storage_gerrit_curve_curve_brpc",
    remote = "http://gerrit.storage.netease.com/curve/curve-brpc",
    commit = "8ed04de5b807b50e0691575916ec96621caad734",
)

bind(
    name = "brpc",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_netease_storage_gerrit_curve_curve_brpc//:bvar",
)

# curve
git_repository(
    name = "com_netease_storage_gerrit_curve_curve",
    remote = "http://gerrit.storage.netease.com/curve",
    commit = "45e8f8887580d367644d3624d7ebd033ebc69503",
)

bind(
    name = "curve",
    actual = "@com_netease_storage_gerrit_curve_curve//src/client:curve",
)

git_repository(
    name = "com_netease_storage_gerrit_curve_curve_braft",
    remote = "http://gerrit.storage.netease.com/curve/curve-braft",
    commit = "27dedf9e2e0d26f99af4e7de35ced5c2106cb711",
)

bind(
    name = "braft",
    actual = "@com_netease_storage_gerrit_curve_curve_braft//:braft",
)

# jsoncpp
new_git_repository(
    name = "jsoncpp",
    build_file = "bazel/jsoncpp.BUILD",
    remote = "https://github.com/open-source-parsers/jsoncpp.git",
    tag = "1.8.4",
)

bind(
    name = "json",
    actual = "@jsoncpp//:json",
)

