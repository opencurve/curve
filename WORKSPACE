workspace(name = "curve")

git_repository(
    name = "com_netease_storage_gerrit_curve_curve_braft",
    remote = "http://gerrit.storage.netease.com/curve/curve-braft",
    commit = "1c229bea30a058722b610c3195972fbfba7ecbcb",
)

bind(
    name = "braft",
    actual = "@com_netease_storage_gerrit_curve_curve_braft//:braft",
)

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
new_git_repository(
    name   = "com_github_google_glog",
    build_file = "bazel/glog.BUILD",
    remote = "https://github.com/google/glog.git",
    tag = "v0.3.5",
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog"
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    tag = "v2.2.1",
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
    commit = "5d7dc6d53af8589d122b67ad0fc2de28f3c2ade5",
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
