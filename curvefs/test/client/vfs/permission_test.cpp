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
 * Created Date: 2023-09-21
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/test/client/filesystem/helper/helper.h"
#include "curvefs/src/client/vfs/permission.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::AttrOption;

class PermissionTest : public ::testing::Test {};

TEST_F(PermissionTest, GetFileMode) {
    auto builder = PermissionBuilder();

    // CASE 1: umask (0022)
    auto perm = builder.SetOption([&](UserPermissionOption* option){
        option->umask = 0022;
    }).Build();
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0666), S_IFREG | 0644);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0777), S_IFREG | 0755);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0600), S_IFREG | 0600);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0777), S_IFDIR | 0755);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0770), S_IFDIR | 0750);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0700), S_IFDIR | 0700);

    // CASE 2: umask (0002)
    perm = builder.SetOption([&](UserPermissionOption* option){
        option->umask = 0002;
    }).Build();
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0666), S_IFREG | 0664);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0777), S_IFREG | 0775);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0600), S_IFREG | 0600);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0777), S_IFDIR | 0775);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0770), S_IFDIR | 0770);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0700), S_IFDIR | 0700);

    // CASE 3: umask (0000)
    perm = builder.SetOption([&](UserPermissionOption* option){
        option->umask = 0;
    }).Build();
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0666), S_IFREG | 0666);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0777), S_IFREG | 0777);
    ASSERT_EQ(perm->GetFileMode(S_IFREG, 0600), S_IFREG | 0600);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0777), S_IFDIR | 0777);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0770), S_IFDIR | 0770);
    ASSERT_EQ(perm->GetFileMode(S_IFDIR, 0700), S_IFDIR | 0700);
}

TEST_F(PermissionTest, WantPermission) {
    auto perm = PermissionBuilder().Build();

    ASSERT_EQ(perm->WantPermission(O_RDONLY), Permission::WANT_READ);
    ASSERT_EQ(perm->WantPermission(O_WRONLY), Permission::WANT_WRITE);
    ASSERT_EQ(perm->WantPermission(O_RDWR),
              Permission::WANT_READ | Permission::WANT_WRITE);
    ASSERT_EQ(perm->WantPermission(O_TRUNC),
              Permission::WANT_READ | Permission::WANT_WRITE);
    ASSERT_EQ(perm->WantPermission(O_RDONLY | O_TRUNC),
              Permission::WANT_READ | Permission::WANT_WRITE);
}

TEST_F(PermissionTest, IsSuperUser) {
    auto builder = PermissionBuilder();

    // CASE 1: I am common user
    auto perm = builder.SetOption([&](UserPermissionOption* option){
        option->uid = 1000;
    }).Build();
    ASSERT_FALSE(perm->IsSuperUser());

    // CASE 2: I am super user
    perm = builder.SetOption([&](UserPermissionOption* option){
        option->uid = 0;
    }).Build();
    ASSERT_TRUE(perm->IsSuperUser());
}

TEST_F(PermissionTest, IsFileOwner) {
    auto builder = PermissionBuilder();
    auto perm = builder.SetOption([&](UserPermissionOption* option){
        option->uid = 1000;
    }).Build();

    // CASE 1: I am file owner
    auto file = MkAttr(100, AttrOption().uid(1000));
    ASSERT_TRUE(perm->IsFileOwner(file));

    // CASE 2: I am not file owner
    file = MkAttr(100, AttrOption().uid(2000));
    ASSERT_FALSE(perm->IsFileOwner(file));
}

TEST_F(PermissionTest, GidInGroup) {
    auto builder = PermissionBuilder();

    // CASE 1: multi-groups
    auto perm = builder.SetOption([&](UserPermissionOption* option){
        option->gids = std::vector<uint32_t>{ 1, 2, 3 };
    }).Build();

    ASSERT_FALSE(perm->GidInGroup(0));
    ASSERT_TRUE(perm->GidInGroup(1));
    ASSERT_TRUE(perm->GidInGroup(2));
    ASSERT_TRUE(perm->GidInGroup(3));
    ASSERT_FALSE(perm->GidInGroup(4));

    // CASE 2: only root group
    perm = builder.SetOption([&](UserPermissionOption* option){
        option->gids = std::vector<uint32_t>{ 0 };
    }).Build();
    ASSERT_TRUE(perm->GidInGroup(0));
    ASSERT_FALSE(perm->GidInGroup(1));
    ASSERT_FALSE(perm->GidInGroup(2));
    ASSERT_FALSE(perm->GidInGroup(3));
}

#define HAS_PERMISSION(OP)                              \
do {                                                    \
    auto rc = perm->Check(file, Permission::WANT_##OP); \
    ASSERT_EQ(rc, CURVEFS_ERROR::OK);                   \
} while (0)

#define NO_PERMISSION(OP)                               \
do {                                                    \
    auto rc = perm->Check(file, Permission::WANT_##OP); \
    ASSERT_EQ(rc, CURVEFS_ERROR::NO_PERMISSION);        \
} while (0)

TEST_F(PermissionTest, Check) {
    // -rwxr-xr-- 1000 1000
    auto file = MkAttr(100, AttrOption().mode(0754).uid(1000).gid(1000));
    auto builder = PermissionBuilder();

    // CASE 1: owner
    auto perm = builder.SetOption([&](UserPermissionOption* option) {
        option->uid = 1000;
        option->gids = std::vector<uint32_t>{ 1000 };
    }).Build();
    HAS_PERMISSION(READ);
    HAS_PERMISSION(WRITE);
    HAS_PERMISSION(EXEC);

    // CASE 2: group
    perm = builder.SetOption([&](UserPermissionOption* option) {
        option->uid = 2000;
        option->gids = std::vector<uint32_t>{ 2000, 1000 };
    }).Build();
    HAS_PERMISSION(READ);
    NO_PERMISSION(WRITE);
    HAS_PERMISSION(EXEC);

    // CASE 3: others
    perm = builder.SetOption([&](UserPermissionOption* option) {
        option->uid = 3000;
        option->gids = std::vector<uint32_t>{ 3000 };
    }).Build();
    HAS_PERMISSION(READ);
    NO_PERMISSION(WRITE);
    NO_PERMISSION(EXEC);

    // CASE 4: superuser
    perm = builder.SetOption([&](UserPermissionOption* option) {
        option->uid = 0;
        option->gids = std::vector<uint32_t>{ 0 };
    }).Build();
    HAS_PERMISSION(READ);
    HAS_PERMISSION(WRITE);
    HAS_PERMISSION(EXEC);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
