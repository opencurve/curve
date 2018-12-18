/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:20:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_LIBCURVE_SPLITOR_H
#define CURVE_LIBCURVE_SPLITOR_H

#include <list>

#include "src/client/metacache.h"
#include "src/client/io_context.h"
#include "src/client/request_context.h"
#include "src/client/client_common.h"
#include "src/client/context_slab.h"

namespace curve {
namespace client {

class Splitor {
 public:
    static int IO2ChunkRequests(IOContext* ioctx,
                            MetaCache* mc,
                            RequestContextSlab* reqslab,
                            std::list<RequestContext*>* targetlist,
                            const char* data,
                            off_t offset,
                            size_t length);
};
}   // namespace client
}   // namespace curve
#endif
