# Description:
#  AWS C++ SDK

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "aws",
    srcs = glob([
        "build/aws-cpp-sdk-s3-crt/libaws-cpp-sdk-s3-crt.so",
        "build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so",
    ]),
    hdrs = glob([
        "aws-cpp-sdk-core/include/**/*.h",
        "aws-cpp-sdk-s3-crt/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-auth/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-common/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-common/include/**/*.inl",
        "crt/aws-crt-cpp/crt/aws-c-common/verification/cbmc/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-io/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-mqtt/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-s3/include/**/*.h",
        "crt/aws-crt-cpp/crt/aws-c-sdkutils/include/**/*.h",
        "crt/aws-crt-cpp/include/**/*.h",
    ]),
    includes = [
        "aws-cpp-sdk-core/include/",
        "aws-cpp-sdk-s3-crt/include/",
        "crt/aws-crt-cpp/crt/aws-c-auth/include",
        "crt/aws-crt-cpp/crt/aws-c-common/include",
        "crt/aws-crt-cpp/crt/aws-c-common/verification/cbmc/include",
        "crt/aws-crt-cpp/crt/aws-c-io/include",
        "crt/aws-crt-cpp/crt/aws-c-mqtt/include",
        "crt/aws-crt-cpp/crt/aws-c-s3/include",
        "crt/aws-crt-cpp/crt/aws-c-sdkutils/include",
        "crt/aws-crt-cpp/include",
    ],
)

