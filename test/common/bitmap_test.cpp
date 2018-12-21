/*
 * Project: curve
 * Created Date: Thursday November 29th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include "src/common/bitmap.h"

namespace curve {
namespace common {

TEST(BitmapTEST, constructor_test) {
    // test constructor
    {
        Bitmap bitmap(32);
        ASSERT_EQ(bitmap.Size(), 32);
        for (int i = 0; i < 32; ++i) {
            ASSERT_FALSE(bitmap.Test(i));
        }
    }

    {
        char* mem = nullptr;
        Bitmap bitmap(20, mem);
        ASSERT_EQ(bitmap.Size(), 20);
        ASSERT_NE(nullptr, bitmap.GetBitmap());
        for (int i = 0; i < 20; ++i) {
            ASSERT_FALSE(bitmap.Test(i));
        }
    }

    {
        char* mem = new char[5];
        memset(mem, 0xff, 5);
        Bitmap bitmap(20, mem);
        ASSERT_EQ(bitmap.Size(), 20);
        ASSERT_NE(mem, bitmap.GetBitmap());
        for (int i = 0; i < 40; ++i) {
            if (i < 20)
                ASSERT_TRUE(bitmap.Test(i));
            else
                ASSERT_FALSE(bitmap.Test(i));
        }
        delete[] mem;
    }
}

TEST(BitmapTEST, basic_test) {
    Bitmap bitmap(32);

    // test Set()
    bitmap.Set();
    ASSERT_EQ(bitmap.NextSetBit(0), 0);
    ASSERT_EQ(bitmap.NextClearBit(0), Bitmap::END_POSITION);

    // test Clear()
    bitmap.Clear();
    ASSERT_EQ(bitmap.NextClearBit(0), 0);
    ASSERT_EQ(bitmap.NextSetBit(0), Bitmap::END_POSITION);

    // test Set(uint32_t index)
    bitmap.Set(7);
    bitmap.Set(8);
    bitmap.Set(31);
    bitmap.Set(32);  // invalid
    ASSERT_EQ(bitmap.NextSetBit(0), 7);
    ASSERT_EQ(bitmap.NextSetBit(7), 7);
    ASSERT_EQ(bitmap.NextSetBit(8), 8);
    ASSERT_EQ(bitmap.NextSetBit(9), 31);
    ASSERT_EQ(bitmap.NextSetBit(32), Bitmap::END_POSITION);

    // test Set(uint32_t startIndex, uint32_t endIndex)
    bitmap.Set(1, 9);
    bitmap.Set(24, 32);  // 32 is invalid
    ASSERT_EQ(bitmap.NextSetBit(0), 1);
    ASSERT_EQ(bitmap.NextClearBit(1), 10);
    ASSERT_EQ(bitmap.NextSetBit(10), 24);
    ASSERT_EQ(bitmap.NextClearBit(24), Bitmap::END_POSITION);

    // test Clear(uint32_t index)
    bitmap.Set();
    bitmap.Clear(7);
    bitmap.Clear(8);
    bitmap.Clear(31);
    bitmap.Clear(32);  // invalid
    ASSERT_EQ(bitmap.NextClearBit(0), 7);
    ASSERT_EQ(bitmap.NextClearBit(7), 7);
    ASSERT_EQ(bitmap.NextClearBit(8), 8);
    ASSERT_EQ(bitmap.NextClearBit(9), 31);
    ASSERT_EQ(bitmap.NextClearBit(32), Bitmap::END_POSITION);

    // test Clear(uint32_t startIndex, uint32_t endIndex)
    bitmap.Set();
    bitmap.Clear(1, 9);
    bitmap.Clear(24, 32);  // 32 is invalid
    ASSERT_EQ(bitmap.NextClearBit(0), 1);
    ASSERT_EQ(bitmap.NextSetBit(1), 10);
    ASSERT_EQ(bitmap.NextClearBit(10), 24);
    ASSERT_EQ(bitmap.NextSetBit(24), Bitmap::END_POSITION);

    // test setting bit which has been setted
    bitmap.Clear();
    bitmap.Clear(1);
    bitmap.Clear(10);
    ASSERT_EQ(bitmap.NextSetBit(0), Bitmap::END_POSITION);

    // test clearing bit which has been cleared
    bitmap.Set();
    bitmap.Set(1);
    bitmap.Set(10);
    ASSERT_EQ(bitmap.NextClearBit(0), Bitmap::END_POSITION);
}

}  // namespace common
}  // namespace curve
