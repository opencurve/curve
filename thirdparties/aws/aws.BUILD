# Description:
#  AWS C++ SDK

load("@curve//:common.bzl", "template_rule")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "aws",
    srcs = glob([
        "aws-cpp-sdk-core/include/**/*.h",
        "aws-cpp-sdk-core/source/*.cpp",
        "aws-cpp-sdk-core/source/auth/**/*.cpp",
        "aws-cpp-sdk-core/source/client/**/*.cpp",
        "aws-cpp-sdk-core/source/config/**/*.cpp",
        "aws-cpp-sdk-core/source/external/**/*.cpp",
        "aws-cpp-sdk-core/source/internal/**/*.cpp",
        "aws-cpp-sdk-core/source/monitoring/*.cpp",
        "aws-cpp-sdk-core/source/net/linux-shared/*.cpp",
        "aws-cpp-sdk-core/source/platform/linux-shared/*.cpp",
        "aws-cpp-sdk-core/source/http/*.cpp",
        "aws-cpp-sdk-core/source/http/curl/**/*.cpp",
        "aws-cpp-sdk-core/source/http/standard/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/*.cpp",
        "aws-cpp-sdk-core/source/utils/base64/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/crypto/*.cpp",
        "aws-cpp-sdk-core/source/utils/crypto/openssl/*.cpp",
        "aws-cpp-sdk-core/source/utils/crypto/factory/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/event/*.cpp",
        "aws-cpp-sdk-core/source/utils/json/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/logging/*.cpp",
        "aws-cpp-sdk-core/source/utils/logging/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/memory/*.cpp",
        "aws-cpp-sdk-core/source/utils/memory/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/stream/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/threading/**/*.cpp",
        "aws-cpp-sdk-core/source/utils/xml/**/*.cpp",
        "aws-cpp-sdk-s3/include/**/*.h",
        "aws-cpp-sdk-s3/source/**/*.cpp",
    ]),
    hdrs = [
        "aws-cpp-sdk-core/include/aws/core/SDKConfig.h",
    ],
    copts = [
        "-DAWS_SDK_VERSION_MAJOR=1",
        "-DAWS_SDK_VERSION_MINOR=7",
        "-DAWS_SDK_VERSION_PATCH=340",
        "-DPLATFORM_LINUX",
        "-DENABLE_CURL_CLIENT",
        "-DENABLE_OPENSSL_ENCRYPTION",
        "-std=c++11",
    ],
    defines = [
        "PLATFORM_LINUX",
    ],
    includes = [
        "aws-cpp-sdk-core/include/",
        "aws-cpp-sdk-s3/include/",
        "aws-cpp-sdk-transfer/include/",
    ],
    linkopts = [
        "-lcurl",
    ],
    deps = [
        "@aws_c_common",
        "@aws_c_event_stream",
        "@aws_checksums",
    ],
)

template_rule(
    name = "SDKConfig_h",
    src = "aws-cpp-sdk-core/include/aws/core/SDKConfig.h.in",
    out = "aws-cpp-sdk-core/include/aws/core/SDKConfig.h",
    substitutions = {
        "cmakedefine": "define",
    },
)
