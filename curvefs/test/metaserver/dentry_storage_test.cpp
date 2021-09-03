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
 * @Project: curve
 * @Date: 2021-06-10 10:04:21
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/dentry_storage.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace metaserver {
class DentryStorageTest : public ::testing::Test {
 protected:
    void SetUp() override { return; }

    void TearDown() override { return; }

    bool CompareDentry(const Dentry &first, const Dentry &second) {
        return first.fsid() == second.fsid() && first.name() == second.name() &&
               first.parentinodeid() == second.parentinodeid() &&
               first.inodeid() == second.inodeid();
    }
};

TEST_F(DentryStorageTest, test1) {
    MemoryDentryStorage storage;
    Dentry dentry1;
    dentry1.set_fsid(1);
    dentry1.set_parentinodeid(2);
    dentry1.set_name("dentry1");
    dentry1.set_inodeid(3);

    Dentry dentry2;
    dentry2.set_fsid(1);
    dentry2.set_parentinodeid(2);
    dentry2.set_name("dentry2");
    dentry2.set_inodeid(4);

    Dentry dentry3;
    dentry3.set_fsid(2);
    dentry3.set_parentinodeid(2);
    dentry3.set_name("dentry3");
    dentry3.set_inodeid(3);

    // TEST INSERT
    ASSERT_EQ(storage.Insert(dentry1), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(dentry2), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(dentry3), MetaStatusCode::OK);
    ASSERT_EQ(storage.Insert(dentry1), MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(storage.Insert(dentry2), MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(storage.Insert(dentry3), MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(storage.Count(), 3);

    // TEST GET
    Dentry temp;
    ASSERT_EQ(storage.Get(DentryKey(dentry1), &temp), MetaStatusCode::OK);
    ASSERT_EQ(dentry1.fsid(), temp.fsid());
    ASSERT_EQ(dentry1.inodeid(), temp.inodeid());
    ASSERT_EQ(dentry1.parentinodeid(), temp.parentinodeid());
    ASSERT_EQ(dentry1.name(), temp.name());
    ASSERT_EQ(storage.Get(DentryKey(dentry2), &temp), MetaStatusCode::OK);
    ASSERT_EQ(dentry2.fsid(), temp.fsid());
    ASSERT_EQ(dentry2.inodeid(), temp.inodeid());
    ASSERT_EQ(dentry2.parentinodeid(), temp.parentinodeid());
    ASSERT_EQ(dentry2.name(), temp.name());
    ASSERT_EQ(storage.Get(DentryKey(dentry3), &temp), MetaStatusCode::OK);
    ASSERT_EQ(dentry3.fsid(), temp.fsid());
    ASSERT_EQ(dentry3.inodeid(), temp.inodeid());
    ASSERT_EQ(dentry3.parentinodeid(), temp.parentinodeid());
    ASSERT_EQ(dentry3.name(), temp.name());

    // TEST LIST
    std::list<Dentry> list;
    ASSERT_EQ(storage.List(DentryParentKey(dentry1), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 2);
    auto it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry1));
    it++;
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    ASSERT_EQ(storage.List(DentryParentKey(dentry2), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 2);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry1));
    it++;
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    ASSERT_EQ(storage.List(DentryParentKey(dentry3), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 1);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry3));

    // TEST DELETE
    ASSERT_EQ(storage.Delete(DentryKey(dentry1)), MetaStatusCode::OK);
    ASSERT_EQ(storage.List(DentryParentKey(dentry1), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 1);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    ASSERT_EQ(storage.List(DentryParentKey(dentry2), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 1);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    ASSERT_EQ(storage.Get(DentryKey(dentry1), &temp),
              MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Get(DentryKey(dentry2), &temp), MetaStatusCode::OK);
    ASSERT_EQ(dentry2.fsid(), temp.fsid());
    ASSERT_EQ(dentry2.inodeid(), temp.inodeid());
    ASSERT_EQ(dentry2.parentinodeid(), temp.parentinodeid());
    ASSERT_EQ(dentry2.name(), temp.name());
    ASSERT_EQ(storage.Get(DentryKey(dentry3), &temp), MetaStatusCode::OK);
    ASSERT_EQ(dentry3.fsid(), temp.fsid());
    ASSERT_EQ(dentry3.inodeid(), temp.inodeid());
    ASSERT_EQ(dentry3.parentinodeid(), temp.parentinodeid());
    ASSERT_EQ(dentry3.name(), temp.name());

    ASSERT_EQ(storage.Delete(DentryKey(dentry3)), MetaStatusCode::OK);
    ASSERT_EQ(storage.List(DentryParentKey(dentry3), &list),
              MetaStatusCode::NOT_FOUND);

    ASSERT_EQ(storage.List(DentryParentKey(dentry2), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 1);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    ASSERT_EQ(storage.List(DentryParentKey(dentry1), &list),
              MetaStatusCode::OK);
    ASSERT_EQ(list.size(), 1);
    it = list.begin();
    ASSERT_TRUE(CompareDentry(*it, dentry2));

    // GetDentryContainer
    std::unordered_map<DentryKey, Dentry, HashDentry> *map =
        storage.GetDentryContainer();
    ASSERT_TRUE(CompareDentry((*map)[DentryKey(dentry2)], dentry2));

    ASSERT_EQ(storage.Delete(DentryKey(dentry2)), MetaStatusCode::OK);
    ASSERT_EQ(storage.Count(), 0);
}
}  // namespace metaserver
}  // namespace curvefs
