
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
 * Date: 2022-02-18
 * Author: Jingli Chen (Wine93)
 */

#include <unordered_map>

#include "curvefs/src/metserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using KeyValue = std::pair<std::string, std::string>;

void TestHGet(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;

    // CASE 1: not found
    Storage::Status s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: return ok
    s = kvStorage->HSet(tablename, "key1", "value1")
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    // CASE 3: get different table or key
    s = kvStorage->HGet("partition:1", "key2", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HGet("partition:2", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: get complex key
    s = kvStorage->HGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HSet(tablename, "1:1", "2:2")
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "2:2");
}

void TestHSet(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;
    Storage::Status s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");
    s = kvStorage->HGet("partition:2", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->HSet("partition:1", "key1", "value2");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value2");

    s = kvStorage->HSet("partition:1", "1:1", "inode1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "inode1");
    s = kvStorage->HGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
}

void TestHDel(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;

    // CASE 1: deleted key not exist, return "not found"
    Storage::Status s = kvStorage->HDel("partition:1", "key1");
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: delete success
    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->HDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: delete one key twice, return "not found"
    s = kvStorage->HDel("partition:1", "key1");
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: delete different table or different key
    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->HDel("partition:2", "key1");
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HDel("partition:1", "key2");
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
}

void TestHGetAll(std::shared_ptr<KVStorage> kvStorage) {
    Storage::Iterator iter;
    size_t size = 0;

    // CASE 1: empty iterator
    iter = kvStorage->HGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(iter.Size(), 0);

    // CASE 2: check key and value for each iterator
    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet("partition:1", "key2", "value2");
    ASSERT_TRUE(s.ok());

    size = 0;
    iter = kvStorage->HGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
        ASSERT_EQ(iter->Key(), "key" + std::to_string(size))
        ASSERT_EQ(iter->Value(), "value" + std::to_string(size))
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(iter.Size(), 2);

    // CASE 3: iterator for different table
    iter = kvStorage->HGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(iter.Size(), 0);

    // CASE 4: iterator for empty key or value
    s = kvStorage->HSet("partition:2", "", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet("partition:2", "key2", "");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet("partition:2", "", "");
    ASSERT_TRUE(s.ok());

    iter = kvStorage->HGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
        if ()
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(iter.Size(), 2);
}

void TestHClear(std::shared_ptr<KVStorage> kvStorage) {
    Iterator iter;
    std::string value;
    size_t size;

    // CASE 1: clear table success
    Storage::Status s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->HClear();
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: iterator after clean
    iter = kvStorage->HGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);
    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);

    // CASE 3: invoke set after clean
    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    iter = kvStorage->HGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 1);
    ASSERT_EQ(iter.Size(), 1);
}

void TestSGet(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;

    // CASE 1: not found
    Storage::Status s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: return ok
    s = kvStorage->SSet(tablename, "key1", "value1")
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    // CASE 3: get different table or key
    s = kvStorage->SGet("partition:1", "key2", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet("partition:2", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: get complex key
    s = kvStorage->SGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->SSet(tablename, "1:1", "2:2")
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "2:2");
}

void TestSSet(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;
    Storage::Status s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");
    s = kvStorage->SGet("partition:2", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->SSet("partition:1", "key1", "value2");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value2");

    s = kvStorage->SSet("partition:1", "1:1", "inode1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "inode1");
    s = kvStorage->SGet("partition:1", "1:1", &value);
    ASSERT_TRUE(s.ok());
}

void TestSDel(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;
    Storage::Status s;

    // CASE 1: deleted key not exist, return "not found"
    s = kvStorage->SDel("partition:1", "key1");
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: delete success
    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->SDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: delete one key twice, return "not found"
    s = kvStorage->SDel("partition:1", "key1");
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: delete different table or different key
    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->SDel("partition:2", "key1");
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SDel("partition:1", "key2");
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
}

// fsId:parentInodeId:name:txid
void TestSRange(std::shared_ptr<KVStorage> kvStorage) {
    size_t size;
    std::string value;
    Storage::Status s;
    Storage::Iterator iter;

    // mock dentry key-value pairs
    // key: fsId:parentInodeId:name:txId
    // value: inodeId:falgs
    std::map<std::string, std::string> pairs{
        { "1:1:/a:0", "100:0" },
        { "1:1:/b:0", "101:0" },
        { "1:1:/b:1", "101:0" },
        { "1:1:/b:2", "101:1" },
        { "1:1:/c:0", "102" },
        { "1:2:/a:0", "200:0" },
        { "1:3:/a:0", "300:0" },
        { "1:3:/a/b:0", "301:0" },
        { "2:1:/a:0", "1000:0" },
        { "3:1:/a:0", "1000:0" },
    };
    for (const auto& v : pairs) {
        s = kvStorage->SSet("partition:1", v.first, v.second);
        ASSERT_TRUE(s.ok());
    }

    // CASE 1: prefix with "fsId:parentInodeId:name:"
    iter = kvStorage->SRange("partition:1", "1:1:/b:")
    ASSERT_EQ(iterator.Status(), 0);
    auto expect = std::vector<KeyValue>{
        KeyValue("1:1:/b:0", "101:0"),
        KeyValue("1:1:/b:1", "101:0"),
        KeyValue("1:1:/b:2", "101:1"),
    };

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_EQ(iter.Key(), expect[size].Key());
        ASSERT_EQ(iter.Value(), expect[size].Value());
        size++;
    }
    ASSERT_EQ(size, 3);

    // CASE 2: prefix with "fsId:parentInodeId:"
    iter = kvStorage->SRange("partition:1", "1:1:")
    ASSERT_EQ(iterator.Status(), 0);
    expect = std::vector<KeyValue>{
        KeyValue("1:1:/a:0", "100:0"),
        KeyValue("1:1:/b:0", "101:0"),
        KeyValue("1:1:/b:1", "101:0"),
        KeyValue("1:1:/b:2", "101:1"),
        KeyValue("1:1:/c:0", "102"),
    };

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_EQ(iter.Key(), expect[size].Key());
        ASSERT_EQ(iter.Value(), expect[size].Value());
        size++;
    }
    ASSERT_EQ(size, 5);

    // CASE 3: get range with different table
    iter = kvStorage->SRange("partition:2", "1:1:/b:")
    ASSERT_EQ(iterator.Status(), 0);

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
}

void TestSGetAll(std::shared_ptr<KVStorage> kvStorage) {
    Storage::Iterator iter;
    size_t size = 0;

    // CASE 1: empty iterator
    iter = kvStorage->SGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);

    // CASE 2: check key and value for each iterator
    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet("partition:1", "key2", "value2");
    ASSERT_TRUE(s.ok());

    size = 0;
    iter = kvStorage->SGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
        ASSERT_EQ(iter->Key(), "key" + std::to_string(size))
        ASSERT_EQ(iter->Value(), "value" + std::to_string(size))
    }
    ASSERT_EQ(size, 2);

    // CASE 3: iterator for different table
    iter = kvStorage->SGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);

    // CASE 4: iterator for empty key or value
    s = kvStorage->SSet("partition:2", "", "value1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet("partition:2", "key2", "");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet("partition:2", "", "");
    ASSERT_TRUE(s.ok());

    iter = kvStorage->SGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);

    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
        if ()
    }
    ASSERT_EQ(size, 2);
}

void TestSClear(std::shared_ptr<KVStorage> kvStorage) {
    Iterator iter;
    std::string value;
    size_t size;

    // CASE 1: clear table success
    Storage::Status s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    s = kvStorage->SClear();
    ASSERT_TRUE(s.ok());

    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: iterator after clean
    iter = kvStorage->SGetAll("partition:2")
    ASSERT_EQ(iterator.Status(), 0);
    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);

    // CASE 3: invoke set after clean
    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    iter = kvStorage->SGetAll("partition:1")
    ASSERT_EQ(iterator.Status(), 0);
    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 1);
    ASSERT_EQ(iter.Size(), 1);
}

void TestMixOperator(std::shared_ptr<KVStorage> kvStorage) {
    std::string value;
    Storage::Status s;
    Storage::Iterator iter;

    // CASE 1: get
    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: set
    s = kvStorage->HSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: del
    s = kvStorage->HDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SDel("partition:1", "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet("partition:1", "key1", "value1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQUAL(value, "value1");

    // CASE 4: range
    iter = kvStorage->SRange("partition:1", "key1");
    ASSERT_EQ(iterator.Status(), 0);
    size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_EQ(iter.Key(), "key1");
        ASSERT_EQ(iter.Value(), "value1");
        size++;
    }
    ASSERT_EQ(size, 0);

    // CASE 5: clear
    s = kvStorage->HDel("partition:1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SDel("partition:1");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs