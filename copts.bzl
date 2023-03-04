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
    "-faligned-new"
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
