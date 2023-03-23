/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: 2022-04-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_METASERVER_STORAGE_UTILS_H_
#define CURVEFS_TEST_METASERVER_STORAGE_UTILS_H_

#include <string>

namespace curvefs {
namespace metaserver {
namespace storage {

std::string RandomString();

std::string RandomStoragePath(const std::string& basedir = "./storage");

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_STORAGE_UTILS_H_
