workspace(name = "curve")

new_git_repository(
    name = "googletest",
    build_file = "bazel/gtest.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

new_git_repository(
    name   = "googlegflags",
    build_file = "bazel/gflags.BUILD",
    commit = "46f73f88b18aee341538c0dfc22b1710a6abedef",
    remote = "https://github.com/gflags/gflags.git",
)

new_http_archive(
    name = "com_github_google_glog",
    build_file = "bazel/glog.BUILD",
    strip_prefix = "glog-a6a166db069520dbbd653c97c2e5b12e08a8bb26",
    url = "https://github.com/google/glog/archive/a6a166db069520dbbd653c97c2e5b12e08a8bb26.tar.gz",
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog",
)