#
#     Copyright (c) 2020 NetEase Inc.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "curvenbd",
    srcs = glob(
        [
            "*.h",
            "*.cpp",
        ],
        exclude = ["main.cpp"],
    ),
    copts = CURVE_DEFAULT_COPTS + [
        "-I /usr/include/libnl3",
    ],
    linkopts = [
        "-lnl-3",
        "-lnl-genl-3",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//nebd/src/part1:nebdclient",
    ],
)

cc_binary(
    name = "curve-nbd",
    srcs = [
        "main.cpp",
    ],
    copts = CURVE_DEFAULT_COPTS + [
        "-I /usr/include/libnl3",
    ],
    deps = [
        "//nbd/src:curvenbd",
        "//src/common:macros",
    ],
)
