/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#ifndef CURVEFS_SRC_METASERVER_S3_METASERVER_S3_H_
#define CURVEFS_SRC_METASERVER_S3_METASERVER_S3_H_

#include <memory>
#include <string>
#include <list>
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace metaserver {
class S3Client {
 public:
    S3Client() {}
    virtual ~S3Client() {}
    virtual void Init(const curve::common::S3AdapterOption& option) = 0;
    virtual int Delete(const std::string& name) = 0;
    virtual int DeleteBatch(const std::list<std::string>& nameList) = 0;
};

class S3ClientImpl : public S3Client {
 public:
    S3ClientImpl() : S3Client() {}
    virtual ~S3ClientImpl() {}

    void SetAdaptor(std::shared_ptr<curve::common::S3Adapter> s3Adapter);
    void Init(const curve::common::S3AdapterOption& option) override;
    /**
     * @brief
     *
     * @param name object_key
     * @return int
     *  1:  object is not exist
     *  0:  delete sucess
     *  -1: delete fail
     * @details
     */
    int Delete(const std::string& name) override;

    int DeleteBatch(const std::list<std::string>& nameList) override;

 private:
    std::shared_ptr<curve::common::S3Adapter> s3Adapter_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3_METASERVER_S3_H_
