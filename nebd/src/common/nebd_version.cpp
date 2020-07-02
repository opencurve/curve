/*
 * Project: nebd
 * Created Date: 2020-03-23
 * Author: charsiu
 * Copyright (c) 2020 netease
 */

#include "nebd/src/common/nebd_version.h"
#include "nebd/src/common/stringstatus.h"

namespace nebd {
namespace common {

// https://gcc.gnu.org/onlinedocs/gcc-4.8.5/cpp/Stringification.html
std::string NebdVersion() {
    static const std::string version =
#ifdef NEBDVERSION
#  define STR(val) #val
#  define XSTR(val) STR(val)
        std::string(XSTR(NEBDVERSION));
#else
        std::string("unknown");
#endif
    return version;
}

const char kNebdMetricPrefix[] = "nebd";
const char kVersion[] = "version";

void ExposeNebdVersion() {
    static StringStatus version;
    version.ExposeAs(kNebdMetricPrefix, kVersion);
    version.Set(kVersion, NebdVersion());
    version.Update();
}

}  // namespace common
}  // namespace nebd
