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

#
# Copyright 2017 The Abseil Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_FLAGS = [
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
]

CXX_FLAGS = [
    "-std=c++11",
]

CURVE_GCC_FLAGS = [
    "-Wall",
    "-Wextra",
    "-Wconversion-null",
    "-Wformat-security",
    "-Woverlength-strings",
    "-Wpointer-arith",
    "-Wundef",
    "-Wunused-local-typedefs",
    "-Wunused-result",
    "-Wvarargs",
    "-Wvla",
    "-Wwrite-strings",
    "-Werror",
    "-DNOMINMAX",
]

CURVE_GCC_TEST_FLAGS = [
    "-Wno-conversion-null",
    "-Wno-deprecated-declarations",
    "-Wno-missing-declarations",
    "-Wno-sign-compare",
    "-Wno-unused-function",
    "-Wno-unused-parameter",
    "-Wno-unused-result",
]

# FIXME: temporary disabled because triggered in many places
CURVE_GCC_DISABLED_FLGAS = [
    "-Wno-error=sign-compare",
    "-Wno-error=reorder",
    "-Wno-error=unused-parameter",
    "-Wno-error=unused-variable",
    "-Wno-error=deprecated-declarations",
    "-Wno-error=narrowing",
]

# FIXME: temporary disabled because triggered in many places
CURVE_GCC_TEST_DISABLED_FLAGS = [
    "-Wno-error=unused-but-set-variable",
    "-Wno-error=vla",
    "-Wno-error=uninitialized",
    "-Wno-error=maybe-uninitialized",
    "-Wno-error=format",
    "-Wno-error=write-strings",
    "-Wno-error=missing-field-initializers",
]

# FIXME: Verify these flags
CURVE_LLVM_FLAGS = [
    "-Wall",
    "-Wextra",
    "-Wcast-qual",
    "-Wconversion",
    "-Wfloat-overflow-conversion",
    "-Wfloat-zero-conversion",
    "-Wfor-loop-analysis",
    "-Wformat-security",
    "-Wgnu-redeclared-enum",
    "-Winfinite-recursion",
    "-Wliteral-conversion",
    "-Wmissing-declarations",
    "-Woverlength-strings",
    "-Wpointer-arith",
    "-Wself-assign",
    "-Wshadow",
    "-Wstring-conversion",
    "-Wtautological-overlap-compare",
    "-Wundef",
    "-Wuninitialized",
    "-Wunreachable-code",
    "-Wunused-comparison",
    "-Wunused-local-typedefs",
    "-Wunused-result",
    "-Wvla",
    "-Wwrite-strings",
    "-Wno-float-conversion",
    "-Wno-float-conversion",
    "-Wno-float-overflow-conversion",
    "-Wno-shorten-64-to-32",
    "-Wno-sign-conversion",
    "-DNOMINMAX",
]

CURVE_LLVM_TEST_FLAGS = [
    "-Wno-c99-extensions",
    "-Wno-deprecated-declarations",
    "-Wno-missing-noreturn",
    "-Wno-missing-prototypes",
    "-Wno-missing-variable-declarations",
    "-Wno-null-conversion",
    "-Wno-shadow",
    "-Wno-shift-sign-overflow",
    "-Wno-sign-compare",
    "-Wno-unused-function",
    "-Wno-unused-member-function",
    "-Wno-unused-parameter",
    "-Wno-unused-private-field",
    "-Wno-unused-template",
    "-Wno-used-but-marked-unused",
    "-Wno-zero-as-null-pointer-constant",
    "-Wno-gnu-zero-variadic-macro-arguments",
    "-Wbraced-scalar-init",
]

# FIXME: temporary disabled because triggered in many places
CURVE_LLVM_DISABLED_FLGAS = [
    "-Wno-c++11-narrowing",
]

CURVE_DEFAULT_COPTS = select({
    "//:clang_compiler": CURVE_LLVM_FLAGS + CXX_FLAGS + BASE_FLAGS + CURVE_LLVM_DISABLED_FLGAS,
    "//conditions:default": CURVE_GCC_FLAGS + CXX_FLAGS + BASE_FLAGS + CURVE_GCC_DISABLED_FLGAS,
})

CURVE_TEST_COPTS = CURVE_DEFAULT_COPTS + select({
    "//:clang_compiler": CURVE_LLVM_TEST_FLAGS,
    "//conditions:default": CURVE_GCC_TEST_FLAGS + CURVE_GCC_TEST_DISABLED_FLAGS,
})

SPDK_COPTS = ["-DWITH_SPDK", "-Iexternal/pfs", "-Iexternal/dpdk", "-Iexternal/spdk"]

SPDK_LINK_OPTS = [
    "-L/usr/local/polarstore/pfsd/lib",
    "-L/usr/local/dpdk/lib",
    "-L/usr/local/spdk/lib",
    "-Wl,-rpath=/usr/local/polarstore/pfsd/lib",
    "-Wl,--whole-archive",
    "-lpfs",
    "-lpfsd_svr",
    "-Wl,--no-whole-archive",
    "-Wl,-rpath=/usr/local/spdk/lib",
    "-Wl,-rpath=/usr/local/dpdk/lib",
    "-Wl,--disable-new-dtags",
    "-Wl,--push-state",
    "-Wl,--no-as-needed",
    "-Wl,--whole-archive",
    "-Wl,--start-group",
    "-Bstatic",
    "-lspdk_bdev_null",
    "-lspdk_bdev_nvme",
    "-lspdk_nvme",
    "-lspdk_env_dpdk",
    "-lspdk_sock_posix",
    "-lspdk_event",
    "-lspdk_event_bdev",
    "-lspdk_bdev",
    "-lspdk_notify",
    "-lspdk_dma",
    "-lspdk_event_accel",
    "-lspdk_accel",
    "-lspdk_event_vmd",
    "-lspdk_vmd",
    "-lspdk_event_sock",
    "-lspdk_init",
    "-lspdk_thread",
    "-lspdk_trace",
    "-lspdk_sock",
    "-lspdk_rpc",
    "-lspdk_jsonrpc",
    "-lspdk_json",
    "-lspdk_util",
    "-lspdk_log",
    "-Wl,--end-group",
    "-Wl,--no-whole-archive",
    "-Wl,--whole-archive",
    "-Wl,--no-as-needed",
    "-Wl,--start-group",
    "-lrte_bus_pci",
    "-lrte_cryptodev",
    "-lrte_dmadev",
    "-lrte_eal",
    "-lrte_ethdev",
    "-lrte_hash",
    "-lrte_kvargs",
    "-lrte_mbuf",
    "-lrte_mempool",
    "-lrte_mempool_ring",
    "-lrte_net",
    "-lrte_pci",
    "-lrte_power",
    "-lrte_rcu",
    "-lrte_ring",
    "-lrte_telemetry",
    "-lrte_vhost",
    "-lrte_meter",
    "-lrte_timer",
    "-lrte_stack",
    "-Wl,--end-group",
    "-Wl,--no-whole-archive",
    "-Wl,--pop-state",
]

SPDK_DEPS = [
    "//external:pfs_headers",
    "//external:dpdk_headers",
    "//external:spdk_headers",
]



