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
 * Project: Curve
 * Created Date: 2023-09-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_BUILDER_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_BUILDER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <memory>

#include "curvefs/src/client/vfs/permission.h"
#include "curvefs/test/client/vfs/helper/mock_fuse_client.h"

namespace curvefs {
namespace client {
namespace vfs {

class PermissionBuilder {
 public:
    using Callback = std::function<void(UserPermissionOption* option)>;

    static UserPermissionOption DefaultOption() {
        return UserPermissionOption {
            uid: 0,
            gids: {0},
            umask: 0222,
        };
    }

 public:
    PermissionBuilder()
        : option_(DefaultOption()) {}

    PermissionBuilder SetOption(Callback callback) {
        callback(&option_);
        return *this;
    }

    std::shared_ptr<Permission> Build() {
        return std::make_shared<Permission>(option_);
    }

 private:
    UserPermissionOption option_;
};

class OperationsBuilder {
 public:
    OperationsBuilder() {}

 private:
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_BUILDER_H_
