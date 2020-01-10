/*
 * Project: curve
 * File Created: 2019-12-10
 * Author: wuhanqing
 * Copyright (c)￼ 2019 netease
 */

#include "src/common/curve_version.h"

#include <bvar/bvar.h>

namespace curve {
namespace common {

// https://gcc.gnu.org/onlinedocs/gcc-4.8.5/cpp/Stringification.html
std::string CurveVersion() {
    static const std::string version =
#ifdef CURVEVERSION
#  define STR(val) #val
#  define XSTR(val) STR(val)
        std::string(XSTR(CURVEVERSION));
#else
        std::string("unknown");
#endif
    return version;
}

void ExposeCurveVersion() {
    static bvar::Status<std::string> version;
    version.expose_as("curve", "version");
    version.set_value(CurveVersion());
}

}  // namespace common
}  // namespace curve
