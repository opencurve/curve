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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/src/client/metaserver_client.h"
#include "curvefs/test/client/mock_metaserver_base_client.h"
#include "curvefs/test/client/mock_metaserver_service.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class MetaServerClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        MetaServerOption metaopt;
        metaopt.msaddr = addr_;
        metaopt.rpcTimeoutMs = 1000;

        ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.Init(metaopt, &mockmsbasecli_));

        ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MetaServerClientImpl msclient_;
    MockMetaServerBaseClient mockmsbasecli_;

    MockMetaServerService mockMetaServerService_;
    std::string addr_ = "127.0.0.1:5701";
    brpc::Server server_;
};

TEST_F(MetaServerClientImplTest, test_GetDentry) {
    uint32_t fsid = 1;
    uint32_t inodeid = 10;
    std::string name = "test_get_dentry";
    curvefs::metaserver::Dentry dout;

    curvefs::metaserver::GetDentryResponse response;
    auto *d = new curvefs::metaserver::Dentry();
    d->set_fsid(fsid);
    d->set_inodeid(inodeid);
    d->set_parentinodeid(1);
    d->set_name(name);
    response.set_allocated_dentry(d);
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, GetDentry(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK,
              msclient_.GetDentry(fsid, inodeid, name, &dout));
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(dout, *d));
}

TEST_F(MetaServerClientImplTest, test_ListDentry) {
    uint32_t fsid = 1;
    uint32_t inodeid = 10;
    std::string last = "test_get_dentry";
    uint32_t count = 1;
    std::list<Dentry> dentryList;

    curvefs::metaserver::ListDentryResponse response;
    auto *d = response.add_dentrys();
    d->set_fsid(fsid);
    d->set_inodeid(inodeid);
    d->set_parentinodeid(1);
    d->set_name("test_get_dentry_1");
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, ListDentry(_, _, _, _, _, _, _))
        .WillOnce(SetArgPointee<4>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK,
              msclient_.ListDentry(fsid, inodeid, last, count, &dentryList));
    ASSERT_EQ(1, dentryList.size());
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        *dentryList.begin(), *d));
}

TEST_F(MetaServerClientImplTest, test_CreateDentry) {
    Dentry dentryCreate;
    dentryCreate.set_fsid(1);
    dentryCreate.set_inodeid(10);
    dentryCreate.set_parentinodeid(1);
    dentryCreate.set_name("test_update_dentry");

    curvefs::metaserver::CreateDentryResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, CreateDentry(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.CreateDentry(dentryCreate));
}

TEST_F(MetaServerClientImplTest, test_DeleteDentry) {
    uint32_t fsid = 1;
    uint64_t inodeid = 10;
    std::string name = "test_delete_dentry";

    curvefs::metaserver::DeleteDentryResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, DeleteDentry(_, _, _, _, _, _))
        .WillOnce(SetArgPointee<3>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.DeleteDentry(fsid, inodeid, name));
}

TEST_F(MetaServerClientImplTest, test_GetInode) {
    uint32_t fsid = 1;
    uint64_t inodeid = 10;
    Inode out;

    curvefs::metaserver::GetInodeResponse response;
    auto inode = new curvefs::metaserver::Inode();
    inode->set_inodeid(inodeid);
    inode->set_fsid(fsid);
    inode->set_length(10);
    inode->set_ctime(1623835517);
    inode->set_mtime(1623835517);
    inode->set_atime(1623835517);
    inode->set_uid(1);
    inode->set_gid(1);
    inode->set_mode(1);
    inode->set_nlink(1);
    inode->set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode->set_symlink("test9");
    response.set_allocated_inode(inode);
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, GetInode(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.GetInode(fsid, inodeid, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(out, *inode));
}

TEST_F(MetaServerClientImplTest, test_UpdateInode) {
    uint32_t fsid = 1;
    uint64_t inodeid = 10;
    Inode inode;
    inode.set_inodeid(inodeid);
    inode.set_fsid(fsid);
    inode.set_length(10);
    inode.set_ctime(1623835517);
    inode.set_mtime(1623835517);
    inode.set_atime(1623835517);
    inode.set_uid(1);
    inode.set_gid(1);
    inode.set_mode(1);
    inode.set_nlink(1);
    inode.set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode.set_symlink("test9");

    curvefs::metaserver::UpdateInodeResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, UpdateInode(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.UpdateInode(inode));
}

TEST_F(MetaServerClientImplTest, test_CreateInode) {
    InodeParam param;
    param.fsId = 1;
    param.length = 10;
    param.uid = 1;
    param.gid = 1;
    param.mode = 1;
    param.type = curvefs::metaserver::FsFileType::TYPE_FILE;
    param.symlink = "test9";
    Inode out;

    curvefs::metaserver::CreateInodeResponse response;
    auto inode = new curvefs::metaserver::Inode();
    inode->set_inodeid(10);
    inode->set_fsid(1);
    inode->set_length(10);
    inode->set_ctime(1623835517);
    inode->set_mtime(1623835517);
    inode->set_atime(1623835517);
    inode->set_uid(1);
    inode->set_gid(1);
    inode->set_mode(1);
    inode->set_nlink(1);
    inode->set_type(curvefs::metaserver::FsFileType::TYPE_FILE);
    inode->set_symlink("test9");
    response.set_allocated_inode(inode);
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, CreateInode(_, _, _, _))
        .WillOnce(SetArgPointee<1>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.CreateInode(param, &out));
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(out, *inode));
}

TEST_F(MetaServerClientImplTest, test_DeleteInode) {
    uint32_t fsId = 1;
    uint64_t inodeid = 10;

    curvefs::metaserver::DeleteInodeResponse response;
    response.set_statuscode(curvefs::metaserver::OK);
    EXPECT_CALL(mockmsbasecli_, DeleteInode(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));

    ASSERT_EQ(CURVEFS_ERROR::OK, msclient_.DeleteInode(fsId, inodeid));
}

}  // namespace client
}  // namespace curvefs
