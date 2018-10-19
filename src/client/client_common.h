/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:24:40 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_COMMON_H
#define CURVE_LIBCURVE_COMMON_H

#include <butil/endpoint.h>
#include <butil/status.h>
#include <braft/configuration.h>

#include <unistd.h>
#include <string>
#include <atomic>

#include "src/client/context_slab.h"

namespace curve {
namespace client {
    using ChunkID = uint64_t;
    using CopysetID = uint32_t;
    using LogicPoolID   = uint32_t;
    using ChunkServerID = uint32_t;
    using ChunkIndex = uint32_t;

    using EndPoint = butil::EndPoint;
    using PeerId    = braft::PeerId;
    using Status    = butil::Status;
    using Configuration = braft::Configuration;

    using RequestContextSlab = ContextSlab<RequestContext>;
}   // namespace client
}   // namespace curve

#endif  // !CURVE_LIBCURVE_COMMON_H
