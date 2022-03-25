/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Sat Aug  7 22:46:58 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/meta_operator_closure.h"

#include <brpc/closure_guard.h>

#include <memory>

#include "curvefs/src/metaserver/copyset/meta_operator.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

void MetaOperatorClosure::Run() {
    std::unique_ptr<MetaOperatorClosure> selfGuard(this);
    std::unique_ptr<MetaOperator> operatorGuard(operator_);
    brpc::ClosureGuard doneGuard(operator_->Closure());

    if (status().ok()) {
        return;
    }

    operator_->RedirectRequest();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
