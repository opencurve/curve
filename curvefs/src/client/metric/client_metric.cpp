/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Fri Apr 21 2023
 * Author: Xinlong-Chen
 */

#include "curvefs/src/client/metric/client_metric.h"

#include <memory>

#include "src/client/client_metric.h"

namespace curvefs {
namespace client {
namespace metric {

using curve::client::CollectMetrics;

const std::string MDSClientMetric::prefix = "curvefs_mds_client";  // NOLINT
const std::string MetaServerClientMetric::prefix = "curvefs_metaserver_client";  // NOLINT
const std::string ClientOpMetric::prefix = "curvefs_client";  // NOLINT
const std::string S3MultiManagerMetric::prefix = "curvefs_client_manager";  // NOLINT
const std::string FSMetric::prefix = "curvefs_client";  // NOLINT
const std::string S3Metric::prefix = "curvefs_s3";  // NOLINT
const std::string DiskCacheMetric::prefix = "curvefs_disk_cache";  // NOLINT
const std::string KVClientManagerMetric::prefix =                  // NOLINT
    "curvefs_kvclient_manager";                                    // NOLINT
const std::string MemcacheClientMetric::prefix =                   // NOLINT
    "curvefs_memcache_client";                                     // NOLINT
const std::string S3ChunkInfoMetric::prefix = "inode_s3_chunk_info";  // NOLINT
const std::string WarmupManagerS3Metric::prefix = "curvefs_warmup";   // NOLINT

void AsyncContextCollectMetrics(
    std::shared_ptr<S3Metric> s3Metric,
    const std::shared_ptr<curve::common::PutObjectAsyncContext>& context) {
    if (s3Metric.get() != nullptr) {
        CollectMetrics(&s3Metric->adaptorWriteS3, context->bufferSize,
                       context->timer.u_elapsed());

        switch (context->type) {
            case curve::common::ContextType::Disk:
                CollectMetrics(&s3Metric->writeToDiskCache, context->bufferSize,
                               context->timer.u_elapsed());
                break;
            case curve::common::ContextType::S3:
                CollectMetrics(&s3Metric->writeToS3, context->bufferSize,
                               context->timer.u_elapsed());
                break;
            default:
                break;
        }
    }
}

void AsyncContextCollectMetrics(
    std::shared_ptr<S3Metric> s3Metric,
    const std::shared_ptr<curve::common::GetObjectAsyncContext>& context) {
    if (s3Metric.get() != nullptr) {
        CollectMetrics(&s3Metric->adaptorReadS3, context->actualLen,
                       context->timer.u_elapsed());

        switch (context->type) {
            case curve::common::ContextType::Disk:
                CollectMetrics(&s3Metric->readFromDiskCache, context->actualLen,
                               context->timer.u_elapsed());
                break;
            case curve::common::ContextType::S3:
                CollectMetrics(&s3Metric->readFromS3, context->actualLen,
                               context->timer.u_elapsed());
                break;
            default:
                break;
        }
    }
}

}  // namespace metric
}  // namespace client
}  // namespace curvefs
