/*
 * Project: curve
 * Created Date: Thursday September 13th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <gtest/gtest.h>
#include "src/nameserver2/namespace_storage.h"


namespace curve {
namespace mds {

TEST(NameSpaceStorageTest, EncodeFileStoreKey) {
    std::string filename = "foo.txt";
    uint64_t parentID = 8;
    std::string str = EncodeFileStoreKey(parentID, filename);

    ASSERT_EQ(str.size(), 17);
    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), FILEINFOKEYPREFIX);
    ASSERT_EQ(str.substr(10, filename.length()), filename);
    for (int i = 2;  i != 9; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[9]), 8);

    parentID = 8 << 8;
    str = EncodeFileStoreKey(parentID, filename);

    ASSERT_EQ(str.size(), 17);
    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), FILEINFOKEYPREFIX);
    ASSERT_EQ(str.substr(10, filename.length()), filename);
    for (int i = 2;  i != 8; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[8]), 8);
    ASSERT_EQ(static_cast<int>(str[9]), 0);
}


TEST(NameSpaceStorageTest, EncodeSegmentStoreKey) {
    uint64_t inodeID = 8;
    offset_t offset = 3 << 16;
    std::string str = EncodeSegmentStoreKey(inodeID, offset);


    ASSERT_EQ(str.substr(0, PREFIX_LENGTH), SEGMENTINFOKEYPREFIX);

    ASSERT_EQ(str.size(), 18);
    for (int i = 2;  i != 9; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[9]), 8);

    for (int i = 10;  i != 15; i++) {
        ASSERT_EQ(static_cast<int>(str[i]), 0);
    }
    ASSERT_EQ(static_cast<int>(str[15]), 3);
    ASSERT_EQ(static_cast<int>(str[16]), 0);
    ASSERT_EQ(static_cast<int>(str[17]), 0);
}

}  // namespace mds
}  // namespace curve
