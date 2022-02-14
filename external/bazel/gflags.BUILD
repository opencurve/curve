#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Bazel (http://bazel.io/) BUILD file for gflags.
#
# See INSTALL.md for instructions for adding gflags to a Bazel workspace.

licenses(["notice"])

exports_files(["src/gflags_completions.sh", "COPYING.txt"])

config_setting(
    name = "x64_windows",
    values = {"cpu": "x64_windows"},
)

load(":bazel/gflags.bzl", "gflags_sources", "gflags_library")

(hdrs, srcs) = gflags_sources(namespace=["gflags", "google"])
gflags_library(hdrs=hdrs, srcs=srcs, threads=0)
gflags_library(hdrs=hdrs, srcs=srcs, threads=1)
