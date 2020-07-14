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

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++14",
    "-I /usr/include/libnl3",
]

cc_library(
    name = "curvenbd",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["main.cpp"]
    ),
    deps = [
        "//external:gflags",
        "//external:glog",
    ],
    copts = COPTS,
    linkopts = [
        "-L/usr/lib/nebd",
        "-lnebdclient",
        "-lclient_proto",
        "-lnebd_common",
        "-lbrpc",
        "-lcc_brpc_idl_options_proto",
        "-lcc_brpc_internal_proto",
        "-lbthread",
        "-lbvar",
        "-ljson2pb",
        "-lmcpack2pb",
        "-lbutil",
        "-lprotoc_lib",
        "-lleveldb",
        "-lprotobuf",
        "-lprotobuf_lite",
        "-lssl",
        "-lrt",
        "-lssl",
        "-lcrypto",
        "-ldl",
        "-lz",
        "-lpthread",
        "-lunwind",
        "-lstdc++",
        "-lm",
        "-lnl-3",
        "-lnl-genl-3",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "curve-nbd",
    srcs = glob([
        "main.cpp"
        ]
    ),
    deps = [
        "//src/tools/nbd:curvenbd"
    ],
    copts = COPTS,
    linkopts = [
        "-Wl,-rpath=/usr/lib/nebd"
    ],
)


