
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

#include <gtest/gtest.h>

#include <unordered_map>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/storage_test.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::metaserver::storage::NameGenerator;
using KeyValue = std::pair<std::string, Dentry>;

Dentry Value(const std::string& name) {
    Dentry dentry;
    dentry.set_fsid(1);
    dentry.set_parentinodeid(1);
    dentry.set_name(name);
    dentry.set_txid(1);
    dentry.set_inodeid(1);
    return dentry;
}

std::string TableName(uint32_t partitionId) {
    auto ng = std::make_shared<NameGenerator>(partitionId);
    return ng->GetDentryTableName();
}

void TestHGet(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: not found
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: return ok
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    // CASE 3: get different table or key
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: get complex key
    s = kvStorage->HGet(TableName(1), "1:1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HSet(TableName(1), "1:1", Value("2:2"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("2:2"));
}

void TestHSet(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: set success
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));
    s = kvStorage->HGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: set one key twice
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HSet(TableName(1), "key1", Value("value2"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value2"));

    // CASE 3: set empty key
    s = kvStorage->HSet(TableName(1), "", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    // CASE 4: set empty value
    s = kvStorage->HSet(TableName(1), "key1", Value(""));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value(""));
}

void TestHDel(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: deleted key not exist, return "ok"
    s = kvStorage->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());

    // CASE 2: delete success
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: delete one key twice, return "ok"
    s = kvStorage->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());

    // CASE 4: delete different table or different key
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HDel(TableName(2), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HDel(TableName(1), "key2");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
}

void TestHGetAll(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    std::shared_ptr<Iterator> iterator;

    // CASE 1: empty iterator
    size = 0;
    iterator = kvStorage->HGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(iterator->Size(), 0);

    // CASE 2: check key and value for each iterator
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(TableName(1), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    size = 0;
    iterator = kvStorage->HGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(kvStorage->HSize(TableName(1)), 2);

    // CASE 3: iterator for different table
    iterator = kvStorage->HGetAll(TableName(2));
    ASSERT_EQ(iterator->Status(), 0);

    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(iterator->Size(), 0);

    // CASE 4: iterator for empty key or value
    s = kvStorage->HSet(TableName(2), "", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(TableName(2), "key2", Value(""));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(TableName(2), "", Value(""));
    ASSERT_TRUE(s.ok());

    iterator = kvStorage->HGetAll(TableName(2));
    ASSERT_EQ(iterator->Status(), 0);

    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(kvStorage->HSize(TableName(2)), 2);
}

void TestHSize(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    Dentry value;

    // CASE 1: get size for empty storage
    size = kvStorage->HSize(TableName(1));
    ASSERT_EQ(size, 0);

    // CASE 2: get size success
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    size = kvStorage->HSize(TableName(1));
    ASSERT_EQ(size, 1);

    // CASE 3: get size for different table
    size = kvStorage->HSize(TableName(2));
    ASSERT_EQ(size, 0);

    // CASE 4: get size after clear
    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());
    size = kvStorage->HSize(TableName(1));
    ASSERT_EQ(size, 0);

    // CASE 5: big size
    for (int i = 0; i < 100; i++) {
        s = kvStorage->HSet(TableName(1),
                            "key" + std::to_string(i),
                            Value("value"));
        ASSERT_TRUE(s.ok());
    }
    size = kvStorage->HSize(TableName(1));
    ASSERT_EQ(size, 100);

    // CASE 6: get size after del
    s = kvStorage->HDel(TableName(1), "key0");
    ASSERT_TRUE(s.ok());
    size = kvStorage->HSize(TableName(1));
    ASSERT_EQ(size, 99);
}

void TestHClear(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    Dentry value;
    std::shared_ptr<Iterator> iterator;

    // CASE 1: clear table success
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: get all after clear table
    iterator = kvStorage->HGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(kvStorage->HSize(TableName(1)), 0);

    // CASE 3: set key-value after clear table
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    iterator = kvStorage->HGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 1);
    ASSERT_EQ(kvStorage->HSize(TableName(1)), 1);

    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());

    // CASE 4: clear different table
    std::string tablename1 = TableName(1);
    std::string tablename2 = TableName(2);
    std::string tablename3 = TableName(3);

    s = kvStorage->HSet(tablename1, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(tablename2, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(tablename3, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());

    // clear table1
    s = kvStorage->HClear(tablename1);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->HSize(tablename1), 0);
    ASSERT_EQ(kvStorage->HSize(tablename2), 1);
    ASSERT_EQ(kvStorage->HSize(tablename3), 1);

    // clear table2
    s = kvStorage->HClear(tablename2);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->HSize(tablename1), 0);
    ASSERT_EQ(kvStorage->HSize(tablename2), 0);
    ASSERT_EQ(kvStorage->HSize(tablename3), 1);

    // clear table3
    s = kvStorage->HClear(tablename3);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->HSize(tablename1), 0);
    ASSERT_EQ(kvStorage->HSize(tablename2), 0);
    ASSERT_EQ(kvStorage->HSize(tablename3), 0);
}

void TestSGet(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: not found
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: return ok
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    // CASE 3: get different table or key
    s = kvStorage->SGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 4: get complex key
    s = kvStorage->SGet(TableName(1), "1:1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->SSet(TableName(1), "1:1", Value("2:2"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "1:1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("2:2"));
}

void TestSSet(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: set success
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: set one key twice
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SSet(TableName(1), "key1", Value("value2"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value2"));

    // CASE 3: set empty key
    s = kvStorage->SSet(TableName(1), "", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    // CASE 4: set empty value
    s = kvStorage->SSet(TableName(1), "key1", Value(""));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value(""));
}

void TestSDel(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;

    // CASE 1: deleted key not exist, return "ok"
    s = kvStorage->SDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());

    // CASE 2: delete success
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: delete one key twice, return "ok"
    s = kvStorage->SDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());

    // CASE 4: delete different table or different key
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SDel(TableName(2), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SDel(TableName(1), "key2");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
}

void TestSSeek(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    Dentry value;
    std::string prefix;
    std::shared_ptr<Iterator> iterator;

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
        s = kvStorage->SSet(TableName(1), v.first, Value(v.second));
        ASSERT_TRUE(s.ok());
    }

    // CASE 1: prefix with "fsId:parentInodeId:name:"
    prefix = "1:1:/b:";
    iterator = kvStorage->SSeek(TableName(1), prefix);
    ASSERT_EQ(iterator->Status(), 0);
    auto expect = std::vector<KeyValue>{
        KeyValue("1:1:/b:0", Value("101:0")),
        KeyValue("1:1:/b:1", Value("101:0")),
        KeyValue("1:1:/b:2", Value("101:1")),
    };

    size = 0;
    for (iterator->SeekToFirst();
        iterator->Valid() && StringStartWith(iterator->Key(), prefix);
        iterator->Next()) {
        ASSERT_EQ(iterator->Key(), expect[size].first);
        ASSERT_TRUE(iterator->ParseFromValue(&value));
        ASSERT_EQ(value, expect[size].second);
        size++;
    }
    ASSERT_EQ(size, 3);

    // CASE 2: prefix with "fsId:parentInodeId:"
    prefix = "1:1:";
    iterator = kvStorage->SSeek(TableName(1), prefix);
    ASSERT_EQ(iterator->Status(), 0);
    expect = std::vector<KeyValue>{
        KeyValue("1:1:/a:0", Value("100:0")),
        KeyValue("1:1:/b:0", Value("101:0")),
        KeyValue("1:1:/b:1", Value("101:0")),
        KeyValue("1:1:/b:2", Value("101:1")),
        KeyValue("1:1:/c:0", Value("102")),
    };

    size = 0;
    for (iterator->SeekToFirst();
        iterator->Valid() && StringStartWith(iterator->Key(), prefix);
        iterator->Next()) {
        ASSERT_EQ(iterator->Key(), expect[size].first);
        ASSERT_TRUE(iterator->ParseFromValue(&value));
        ASSERT_EQ(value, expect[size].second);
        size++;
    }
    ASSERT_EQ(size, 5);

    // CASE 3: get range with different table
    prefix = "1:1:/b:";
    iterator = kvStorage->SSeek(TableName(2), prefix);
    ASSERT_EQ(iterator->Status(), 0);

    size = 0;
    for (iterator->SeekToFirst();
        iterator->Valid() && StringStartWith(iterator->Key(), prefix);
        iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
}

void TestSGetAll(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size = 0;
    Dentry value;
    std::shared_ptr<Iterator> iterator;

    // CASE 1: empty iterator
    iterator = kvStorage->SGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(kvStorage->SSize(TableName(1)), 0);

    // CASE 2: check key and value for each iterator
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(1), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    size = 0;
    iterator = kvStorage->SGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
        ASSERT_EQ(iterator->Key(), "key" + std::to_string(size));
        ASSERT_TRUE(iterator->ParseFromValue(&value));
        ASSERT_EQ(value, Value("value" + std::to_string(size)));
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(kvStorage->SSize(TableName(1)), 2);

    // CASE 3: iterator for different table
    iterator = kvStorage->SGetAll(TableName(2));
    ASSERT_EQ(iterator->Status(), 0);

    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(kvStorage->SSize(TableName(2)), 0);

    // CASE 4: iterator for empty key or value
    s = kvStorage->SSet(TableName(2), "", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(2), "key2", Value(""));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(2), "", Value(""));
    ASSERT_TRUE(s.ok());

    iterator = kvStorage->SGetAll(TableName(2));
    ASSERT_EQ(iterator->Status(), 0);

    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 2);
    ASSERT_EQ(kvStorage->SSize(TableName(2)), 2);
}

void TestSSize(std::shared_ptr<KVStorage> kvStorage) {
    size_t size;
    Dentry value;
    Status s;

    // CASE 1: get size for empty storage
    size = kvStorage->SSize(TableName(1));
    ASSERT_EQ(size, 0);

    // CASE 2: get size success
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    size = kvStorage->SSize(TableName(1));
    ASSERT_EQ(size, 1);

    // CASE 3: get size for different table
    size = kvStorage->SSize(TableName(2));
    ASSERT_EQ(size, 0);

    // CASE 4: get size after clear
    s = kvStorage->SClear(TableName(1));
    ASSERT_TRUE(s.ok());
    size = kvStorage->SSize(TableName(1));
    ASSERT_EQ(size, 0);

    // CASE 5: big size
    for (int i = 0; i < 100; i++) {
        s = kvStorage->SSet(TableName(1),
                            "key" + std::to_string(i),
                            Value("value"));
        ASSERT_TRUE(s.ok());
    }
    size = kvStorage->SSize(TableName(1));
    ASSERT_EQ(size, 100);

    // CASE 6: get size after del
    s = kvStorage->SDel(TableName(1), "key0");
    ASSERT_TRUE(s.ok());
    size = kvStorage->SSize(TableName(1));
    ASSERT_EQ(size, 99);
}

void TestSClear(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    Dentry value;
    std::shared_ptr<Iterator> iterator;

    // CASE 1: clear table success
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    s = kvStorage->SClear(TableName(1));
    ASSERT_TRUE(s.ok());

    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: get all after clear table
    iterator = kvStorage->SGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 0);
    ASSERT_EQ(kvStorage->SSize(TableName(1)), 0);

    // CASE 3: set key-value after clear table
    s = kvStorage->SSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    iterator = kvStorage->SGetAll(TableName(1));
    ASSERT_EQ(iterator->Status(), 0);
    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    ASSERT_EQ(size, 1);
    ASSERT_EQ(kvStorage->SSize(TableName(1)), 1);

    // CASE 4: clear different table
    std::string tablename1 = TableName(1);
    std::string tablename2 = TableName(2);
    std::string tablename3 = TableName(3);

    s = kvStorage->SSet(tablename1, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(tablename2, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(tablename3, "key1", Value("value1"));
    ASSERT_TRUE(s.ok());

    // clear table1
    s = kvStorage->SClear(tablename1);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->SSize(tablename1), 0);
    ASSERT_EQ(kvStorage->SSize(tablename2), 1);
    ASSERT_EQ(kvStorage->SSize(tablename3), 1);

    // clear table2
    s = kvStorage->SClear(tablename2);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->SSize(tablename1), 0);
    ASSERT_EQ(kvStorage->SSize(tablename2), 0);
    ASSERT_EQ(kvStorage->SSize(tablename3), 1);

    // clear table3
    s = kvStorage->SClear(tablename3);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->SSize(tablename1), 0);
    ASSERT_EQ(kvStorage->SSize(tablename2), 0);
    ASSERT_EQ(kvStorage->SSize(tablename3), 0);
}

void TestMixOperator(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    size_t size;
    Dentry value;
    std::shared_ptr<Iterator> iterator;

    // CASE 1: get
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 2: set
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 3: del
    s = kvStorage->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SDel(TableName(2), "key1");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(2), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));

    // CASE 4: range
    iterator = kvStorage->SSeek(TableName(2), "key1");
    ASSERT_EQ(iterator->Status(), 0);
    size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        ASSERT_EQ(iterator->Key(), "key1");
        ASSERT_TRUE(iterator->ParseFromValue(&value));
        ASSERT_EQ(value, Value("value1"));
        size++;
    }
    ASSERT_EQ(size, 1);

    // CASE 5: clear
    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SClear(TableName(2));
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->SGet(TableName(2), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());

    // CASE 6: size
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(2), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(kvStorage->HSize(TableName(1)), 1);
    ASSERT_EQ(kvStorage->SSize(TableName(2)), 1);
}

void TestTransaction(std::shared_ptr<KVStorage> kvStorage) {
    Status s;
    Dentry value;
    std::shared_ptr<Iterator> iterator;
    std::shared_ptr<StorageTransaction> txn;

    // CASE 1: not in transaction
    txn = std::make_shared<RocksDBStorage>();
    s = txn->Commit();
    ASSERT_TRUE(s.IsNotSupported());

    s = txn->Rollback();
    ASSERT_TRUE(s.IsNotSupported());

    // CASE 2: commit transaction
    txn = kvStorage->BeginTransaction();
    s = txn->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = txn->HSet(TableName(1), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    // keys are not found before transaction commit
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = txn->Commit();
    ASSERT_TRUE(s.ok());

    // get keys success after transaction commit
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value2"));

    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());

    // CASE 3: rollback transaction
    txn = kvStorage->BeginTransaction();
    s = txn->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = txn->HSet(TableName(1), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    s = txn->Rollback();
    ASSERT_TRUE(s.ok());

    // keys are not found after transaction rollback
    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = kvStorage->HClear(TableName(1));
    ASSERT_TRUE(s.ok());

    // CASE 4: transaction for delete
    s = kvStorage->HSet(TableName(1), "key1", Value("value1"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->HSet(TableName(1), "key2", Value("value2"));
    ASSERT_TRUE(s.ok());

    txn = kvStorage->BeginTransaction();
    s = txn->HDel(TableName(1), "key1");
    ASSERT_TRUE(s.ok());
    s = txn->HDel(TableName(1), "key2");
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value1"));
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(value, Value("value2"));

    // commit transaction
    s = txn->Commit();
    ASSERT_TRUE(s.ok());

    s = kvStorage->HGet(TableName(1), "key1", &value);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage->HGet(TableName(1), "key2", &value);
    ASSERT_TRUE(s.IsNotFound());
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
