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

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_library(
    name = "mock_lib",
    srcs = glob([
        "mock_*",
        "fake_*",
    ]),
    hdrs = glob(["*.h"]),
    copts = CURVE_TEST_COPTS + [
        "-I /usr/include/libnl3",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gtest",
        "//nbd/src:curvenbd",
    ],
)

cc_binary(
    name = "nbd_test",
    srcs = glob([
        "*.cpp",
    ]),
    copts = CURVE_TEST_COPTS + [
        "-I /usr/include/libnl3",
    ],
    deps = [
        "//nbd/src:curvenbd",
        "//nbd/test:mock_lib",
    ],
)
