/*
 * Project: curve
 * File Created: Friday, 16th November 2018 9:57:26 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <list>

#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.h"


using curve::chunkserver::CSDataStore;
using curve::chunkserver::CSSfsAdaptor;
using curve::chunkserver::FsAdaptorUtil;
using curve::chunkserver::CSSfsLocalFsImpl;

TEST(AdaptorUtil, ParserUriTest) {
    std::string protocol = "local:/";
    std::string ret;
    std::string proto = FsAdaptorUtil::ParserUri(protocol, &ret);
    ASSERT_STREQ("", proto.c_str());

    protocol = "local://";
    proto = FsAdaptorUtil::ParserUri(protocol, &ret);
    ASSERT_STREQ("local", proto.c_str());
}

TEST(AdaptorUtil, ParserDirPath) {
    std::string path = "./test1/test2";
    std::list<std::string> pathlist;
    pathlist = FsAdaptorUtil::ParserDirPath(path);
    std::string dir1 = pathlist.front();
    std::string dir2 = pathlist.back();
    ASSERT_STREQ("./test1", dir1.c_str());
    ASSERT_STREQ("./test1/test2", dir2.c_str());
}

TEST(SfsAdaptor, InitializeTest) {
    CSSfsAdaptor csfsada;
    std::string uri = "bluestore://";
    ASSERT_FALSE(csfsada.Initialize("", uri));
    uri = "sfs://";
    ASSERT_FALSE(csfsada.Initialize("", uri));

    uri = "notexist://./";
    ASSERT_FALSE(csfsada.Initialize("", uri));

    uri = "local://./";
    ASSERT_TRUE(csfsada.Initialize("", uri));
    ASSERT_TRUE(csfsada.Initialize("", uri));
}

TEST(Datastore, InitializeTwice) {
    auto csfsadaptr = std::shared_ptr<CSSfsAdaptor>(new CSSfsAdaptor);
    auto uri = "local://./";
    ASSERT_TRUE(csfsadaptr->Initialize("", uri));

    std::string copysetpath = "./test1//";
    CSDataStore datastore;
    ASSERT_TRUE(datastore.Initialize(csfsadaptr, copysetpath));
    ASSERT_TRUE(datastore.Initialize(csfsadaptr, copysetpath));
    csfsadaptr->Rmdir("./test1/data");
    csfsadaptr->Rmdir("./test1");
}

TEST(DataStore, Exceptiontest) {
    auto csfsadaptr = std::shared_ptr<CSSfsAdaptor>(new CSSfsAdaptor);
    auto uri = "local://./";
    ASSERT_TRUE(csfsadaptr->Initialize("", uri));

    std::string copysetpath = "./test1//";
    CSDataStore datastore;
    ASSERT_TRUE(datastore.Initialize(csfsadaptr, copysetpath));
    csfsadaptr->Rmdir("./test1/data");
    csfsadaptr->Rmdir("./test1");
}
