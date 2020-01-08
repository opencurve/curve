/*
 * Project: curve
 * File Created: Saturday, 12th October 2019 10:49:57 am
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>  // NOLINT

#include "src/client/session_map.h"

using curve::client::SessionMap;


TEST(SessionMapTest, LoadSessionMapAndGetFileSessionIDTest) {
    SessionMap sm;
    const std::string sessionpath = "./test/client/testConfig/sessionmap.json";
    const std::string path2 = "./test/client/testConfig/sessionmap2.json";

    ASSERT_STREQ("123456789", sm.GetFileSessionID(sessionpath, "/root/test1.txt").c_str());     // NOLINT
    ASSERT_STRNE("123456789", sm.GetFileSessionID(sessionpath, "/root/test2.txt").c_str());     // NOLINT

    ASSERT_STRNE("123456789", sm.GetFileSessionID(path2, "/root/test1.txt").c_str());     // NOLINT
    ASSERT_STRNE("123456789", sm.GetFileSessionID(path2, "/root/test2.txt").c_str());     // NOLINT
}

TEST(SessionMapTest, PersistSessionMapWithLockTest) {
    SessionMap sm;
    const std::string sessionpath = "./test/client/testConfig/sessionmap.json";
    ASSERT_EQ(0, sm.PersistSessionMapWithLock(sessionpath,
                                             "/root/test3.txt",
                                              "999999999"));
    ASSERT_STREQ("999999999", sm.GetFileSessionID(sessionpath, "/root/test3.txt").c_str());     // NOLINT

    ASSERT_EQ(0, sm.PersistSessionMapWithLock(sessionpath,
                                             "/root/test3.txt",
                                              "888888888"));

    ASSERT_STREQ("888888888", sm.GetFileSessionID(sessionpath, "/root/test3.txt").c_str());     // NOLINT
}

TEST(SessionMapTest, DelSessionMapTest) {
    SessionMap sm;
    const std::string sessionpath = "./test/client/testConfig/sessionmaptest";
    for (int i = 0; i < 100; i++) {
        std::string filename = std::string("/") + std::to_string(i);
        std::string sessionid = std::to_string(i);
        ASSERT_EQ(0, sm.PersistSessionMapWithLock(sessionpath,
                                                  filename, sessionid));
    }

    auto delfuc1 = [&](){
        for (int i = 0; i < 50; i++) {
            SessionMap sm;
            std::string filename = std::string("/") + std::to_string(i);
            LOG(INFO) << "delete filesession [" << filename << "]";
            ASSERT_EQ(0, sm.DelSessionID(sessionpath, filename));
        }
    };

    auto delfuc2 = [&](){
        for (int i = 50; i < 100; i++) {
            SessionMap sm;
            std::string filename = std::string("/") + std::to_string(i);
            LOG(INFO) << "delete filesession [" << filename << "]";
            ASSERT_EQ(0, sm.DelSessionID(sessionpath, filename));
        }
    };

    std::thread t1(delfuc1);
    std::thread t2(delfuc2);

    t1.join();
    t2.join();

    for (int i = 0; i < 100; i++) {
        std::string filename = std::string("/") + std::to_string(i);
        ASSERT_STREQ("", sm.GetFileSessionID(sessionpath, filename).c_str());
    }
}

TEST(SessionMapTest, InvalidParam) {
    SessionMap sm;
    std::string path = "/x/y/x";

    // invalid path
    ASSERT_STREQ("", sm.GetFileSessionID(path, "").c_str());
    ASSERT_EQ(-1, sm.PersistSessionMapWithLock(path, "", ""));
    ASSERT_EQ(-1, sm.DelSessionID(path, ""));
}
