workspace(name = "curve")

local_repository(
    name = "com_github_brpc_braft",
    path = "thirdparties/braft",
)

bind(
    name = "braft",
    actual = "@com_github_brpc_braft//:braft",
)

# gtest deps
http_archive(
    name = "com_google_googletest",
    strip_prefix = "googletest-0fe96607d85cf3a25ac40da369db62bbee2939a5",
    url = "https://github.com/google/googletest/archive/0fe96607d85cf3a25ac40da369db62bbee2939a5.tar.gz",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

# glog deps
new_git_repository(
    name = "com_github_glog_glog",
    build_file = "bazel/glog.BUILD",
    remote = "https://github.com/google/glog.git",
    tag = "v0.3.5",
)

bind(
    name = "glog",
    actual = "@com_github_glog_glog//:glog",
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

# protobuf
http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-ab8edf1dbe2237b4717869eaab11a2998541ad8d",
    url = "https://github.com/google/protobuf/archive/ab8edf1dbe2237b4717869eaab11a2998541ad8d.tar.gz",
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

# leveldb
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

# brpc
local_repository(
    name = "com_github_brpc_brpc",
    path = "thirdparties/brpc",
)

bind(
    name = "brpc",
    actual = "@com_github_brpc_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_github_brpc_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_github_brpc_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_github_brpc_brpc//:bvar",
)

