
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
 * Date: 2022-02-28
 * Author: Jingli Chen (Wine93)
 */

#include <unordered_map>

#include "curvefs/src/metserver/storage/storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

static void TestHGet(std::shared_ptr<KVStorage> kvStorage);
static void TestHSet(std::shared_ptr<KVStorage> kvStorage);
static void TestHDel(std::shared_ptr<KVStorage> kvStorage);
static void TestHGetAll(std::shared_ptr<KVStorage> kvStorage);
static void TestHClear(std::shared_ptr<KVStorage> kvStorage);

static void TestSGet(std::shared_ptr<KVStorage> kvStorage);
static void TestSSet(std::shared_ptr<KVStorage> kvStorage);
static void TestSDel(std::shared_ptr<KVStorage> kvStorage);
static void TestSRange(std::shared_ptr<KVStorage> kvStorage);
static void TestSGetAll(std::shared_ptr<KVStorage> kvStorage);
static void TestSClear(std::shared_ptr<KVStorage> kvStorage);

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs