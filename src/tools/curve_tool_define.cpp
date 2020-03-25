/*
 * Project: curve
 * Created Date: 2020-03-16
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include "src/tools/curve_tool_define.h"

DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
DEFINE_string(mdsDummyPort, "6667", "dummy port of mds, "
                                    "can specify one or several."
                                    "if specify several, the order "
                                    "should be the same as mds addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
