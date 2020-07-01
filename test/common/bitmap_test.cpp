/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thursday November 29th 2018
 * Author: yangyaokai
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

    // 测试拷贝构造
    {
        Bitmap bitmap1(32);
        Bitmap bitmap2(bitmap1);
        ASSERT_EQ(bitmap2.Size(), 32);
        for (int i = 0; i < 32; ++i) {
            ASSERT_FALSE(bitmap2.Test(i));
        }
    }

    // 测试赋值操作
    {
        Bitmap bitmap1(32);
        Bitmap bitmap2(16);
        bitmap2 = bitmap1;
        Bitmap bitmap3 = bitmap1;
        ASSERT_EQ(bitmap2.Size(), 32);
        for (int i = 0; i < 32; ++i) {
            ASSERT_FALSE(bitmap2.Test(i));
        }
        ASSERT_EQ(bitmap3.Size(), 32);
        for (int i = 0; i < 32; ++i) {
            ASSERT_FALSE(bitmap3.Test(i));
        }
    }

    // 测试比较操作符
    {
        Bitmap bitmap1(16);
        Bitmap bitmap2(16);
        Bitmap bitmap3(32);
        ASSERT_TRUE(bitmap1 == bitmap2);
        ASSERT_FALSE(bitmap1 != bitmap2);
        ASSERT_FALSE(bitmap1 == bitmap3);
        ASSERT_TRUE(bitmap1 != bitmap3);

        bitmap1.Set(1);
        ASSERT_FALSE(bitmap1 == bitmap2);
        ASSERT_TRUE(bitmap1 != bitmap2);
        ASSERT_FALSE(bitmap1 == bitmap3);
        ASSERT_TRUE(bitmap1 != bitmap3);

        bitmap1.Set();
        bitmap2.Set();
        bitmap3.Set();
        ASSERT_TRUE(bitmap1 == bitmap2);
        ASSERT_FALSE(bitmap1 != bitmap2);
        ASSERT_FALSE(bitmap1 == bitmap3);
        ASSERT_TRUE(bitmap1 != bitmap3);

        bitmap1.Clear();
        bitmap2.Clear();
        bitmap3.Clear();
        ASSERT_TRUE(bitmap1 == bitmap2);
        ASSERT_FALSE(bitmap1 != bitmap2);
        ASSERT_FALSE(bitmap1 == bitmap3);
        ASSERT_TRUE(bitmap1 != bitmap3);
    }
}

TEST(BitmapTEST, basic_test) {
    Bitmap bitmap(32);

    // test Set()
    bitmap.Set();
    ASSERT_EQ(bitmap.NextSetBit(0), 0);
    ASSERT_EQ(bitmap.NextSetBit(0, 32), 0);
    ASSERT_EQ(bitmap.NextClearBit(0), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextClearBit(0, 31), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextClearBit(0, 32), Bitmap::NO_POS);

    // test Clear()
    bitmap.Clear();
    ASSERT_EQ(bitmap.NextClearBit(0), 0);
    ASSERT_EQ(bitmap.NextClearBit(0, 32), 0);
    ASSERT_EQ(bitmap.NextSetBit(0), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextSetBit(0, 31), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextSetBit(0, 32), Bitmap::NO_POS);

    // test Set(uint32_t index)
    bitmap.Set(7);
    bitmap.Set(8);
    bitmap.Set(31);
    bitmap.Set(32);  // invalid
    ASSERT_EQ(bitmap.NextSetBit(0), 7);
    ASSERT_EQ(bitmap.NextSetBit(7), 7);
    ASSERT_EQ(bitmap.NextSetBit(8), 8);
    ASSERT_EQ(bitmap.NextSetBit(9), 31);
    ASSERT_EQ(bitmap.NextSetBit(32), Bitmap::NO_POS);

    // test NextSetBit(startIndex, endIndex);
    ASSERT_EQ(bitmap.NextSetBit(0, 6), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextSetBit(0, 7), 7);
    ASSERT_EQ(bitmap.NextSetBit(8, 7), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextSetBit(8, 8), 8);
    ASSERT_EQ(bitmap.NextSetBit(8, 32), 8);
    ASSERT_EQ(bitmap.NextSetBit(9, 32), 31);
    ASSERT_EQ(bitmap.NextSetBit(32, 33), Bitmap::NO_POS);

    // test NextClearBit(startIndex, endIndex);
    ASSERT_EQ(bitmap.NextClearBit(0, 32), 0);
    ASSERT_EQ(bitmap.NextClearBit(7, 32), 9);
    ASSERT_EQ(bitmap.NextClearBit(9, 32), 9);
    ASSERT_EQ(bitmap.NextClearBit(31, 32), Bitmap::NO_POS);

    // test Set(uint32_t startIndex, uint32_t endIndex)
    bitmap.Set(1, 9);
    bitmap.Set(24, 32);  // 32 is invalid
    ASSERT_EQ(bitmap.NextSetBit(0), 1);
    ASSERT_EQ(bitmap.NextClearBit(1), 10);
    ASSERT_EQ(bitmap.NextSetBit(10), 24);
    ASSERT_EQ(bitmap.NextClearBit(24), Bitmap::NO_POS);

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
    ASSERT_EQ(bitmap.NextClearBit(32), Bitmap::NO_POS);

    // test NextClearBit(startIndex, endIndex);
    ASSERT_EQ(bitmap.NextClearBit(0, 6), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextClearBit(0, 7), 7);
    ASSERT_EQ(bitmap.NextClearBit(8, 7), Bitmap::NO_POS);
    ASSERT_EQ(bitmap.NextClearBit(8, 8), 8);
    ASSERT_EQ(bitmap.NextClearBit(8, 32), 8);
    ASSERT_EQ(bitmap.NextClearBit(9, 32), 31);
    ASSERT_EQ(bitmap.NextClearBit(32, 33), Bitmap::NO_POS);

    // test NextSetBit(startIndex, endIndex);
    ASSERT_EQ(bitmap.NextSetBit(0, 32), 0);
    ASSERT_EQ(bitmap.NextSetBit(7, 32), 9);
    ASSERT_EQ(bitmap.NextSetBit(9, 32), 9);
    ASSERT_EQ(bitmap.NextSetBit(31, 32), Bitmap::NO_POS);

    // test Clear(uint32_t startIndex, uint32_t endIndex)
    bitmap.Set();
    bitmap.Clear(1, 9);
    bitmap.Clear(24, 32);  // 32 is invalid
    ASSERT_EQ(bitmap.NextClearBit(0), 1);
    ASSERT_EQ(bitmap.NextSetBit(1), 10);
    ASSERT_EQ(bitmap.NextClearBit(10), 24);
    ASSERT_EQ(bitmap.NextSetBit(24), Bitmap::NO_POS);

    // test setting bit which has been setted
    bitmap.Clear();
    bitmap.Clear(1);
    bitmap.Clear(10);
    ASSERT_EQ(bitmap.NextSetBit(0), Bitmap::NO_POS);

    // test clearing bit which has been cleared
    bitmap.Set();
    bitmap.Set(1);
    bitmap.Set(10);
    ASSERT_EQ(bitmap.NextClearBit(0), Bitmap::NO_POS);
}

TEST(BitmapTEST, divide_test) {
    Bitmap bitmap(32);
    vector<BitRange> clearRanges;
    vector<BitRange> setRanges;

    // 所有位为0
    {
        bitmap.Clear();
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(0, setRanges.size());
        ASSERT_EQ(0, clearRanges[0].beginIndex);
        ASSERT_EQ(31, clearRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 所有位为1
    {
        bitmap.Set();
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(0, clearRanges.size());
        ASSERT_EQ(1, setRanges.size());
        ASSERT_EQ(0, setRanges[0].beginIndex);
        ASSERT_EQ(31, setRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 两个range，起始为clear range，末尾为set range
    {
        bitmap.Clear(0, 16);
        bitmap.Set(17, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(1, setRanges.size());
        ASSERT_EQ(0, clearRanges[0].beginIndex);
        ASSERT_EQ(16, clearRanges[0].endIndex);
        ASSERT_EQ(17, setRanges[0].beginIndex);
        ASSERT_EQ(31, setRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 两个range，起始为 set range，末尾为 clear range
    {
        bitmap.Set(0, 16);
        bitmap.Clear(17, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(1, setRanges.size());
        ASSERT_EQ(0, setRanges[0].beginIndex);
        ASSERT_EQ(16, setRanges[0].endIndex);
        ASSERT_EQ(17, clearRanges[0].beginIndex);
        ASSERT_EQ(31, clearRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 三个range，头尾为 set range，中间为 clear range
    {
        bitmap.Set(0, 8);
        bitmap.Clear(9, 25);
        bitmap.Set(26, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(0, setRanges[0].beginIndex);
        ASSERT_EQ(8, setRanges[0].endIndex);
        ASSERT_EQ(26, setRanges[1].beginIndex);
        ASSERT_EQ(31, setRanges[1].endIndex);
        ASSERT_EQ(9, clearRanges[0].beginIndex);
        ASSERT_EQ(25, clearRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 三个range，头尾为 clear range，中间为 set range
    {
        bitmap.Clear(0, 8);
        bitmap.Set(9, 25);
        bitmap.Clear(26, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(2, clearRanges.size());
        ASSERT_EQ(1, setRanges.size());
        ASSERT_EQ(0, clearRanges[0].beginIndex);
        ASSERT_EQ(8, clearRanges[0].endIndex);
        ASSERT_EQ(26, clearRanges[1].beginIndex);
        ASSERT_EQ(31, clearRanges[1].endIndex);
        ASSERT_EQ(9, setRanges[0].beginIndex);
        ASSERT_EQ(25, setRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 四个range，头为 clear range，末尾为 set range
    {
        bitmap.Clear(0, 7);
        bitmap.Set(8, 15);
        bitmap.Clear(16, 23);
        bitmap.Set(24, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(2, clearRanges.size());
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(0, clearRanges[0].beginIndex);
        ASSERT_EQ(7, clearRanges[0].endIndex);
        ASSERT_EQ(16, clearRanges[1].beginIndex);
        ASSERT_EQ(23, clearRanges[1].endIndex);
        ASSERT_EQ(8, setRanges[0].beginIndex);
        ASSERT_EQ(15, setRanges[0].endIndex);
        ASSERT_EQ(24, setRanges[1].beginIndex);
        ASSERT_EQ(31, setRanges[1].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 四个range，头为 set range，末尾为 clear range
    {
        bitmap.Set(0, 7);
        bitmap.Clear(8, 15);
        bitmap.Set(16, 23);
        bitmap.Clear(24, 31);
        bitmap.Divide(0, 31, &clearRanges, &setRanges);
        ASSERT_EQ(2, clearRanges.size());
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(0, setRanges[0].beginIndex);
        ASSERT_EQ(7, setRanges[0].endIndex);
        ASSERT_EQ(16, setRanges[1].beginIndex);
        ASSERT_EQ(23, setRanges[1].endIndex);
        ASSERT_EQ(8, clearRanges[0].beginIndex);
        ASSERT_EQ(15, clearRanges[0].endIndex);
        ASSERT_EQ(24, clearRanges[1].beginIndex);
        ASSERT_EQ(31, clearRanges[1].endIndex);
        clearRanges.clear();
        setRanges.clear();
    }

    // 复杂场景随机偏移测试
    {
        bitmap.Set(0, 5);
        bitmap.Clear(6, 9);
        bitmap.Set(10, 18);
        bitmap.Clear(19, 25);
        bitmap.Set(26, 31);

        bitmap.Divide(3, 13, &clearRanges, &setRanges);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(3, setRanges[0].beginIndex);
        ASSERT_EQ(5, setRanges[0].endIndex);
        ASSERT_EQ(10, setRanges[1].beginIndex);
        ASSERT_EQ(13, setRanges[1].endIndex);
        ASSERT_EQ(6, clearRanges[0].beginIndex);
        ASSERT_EQ(9, clearRanges[0].endIndex);
        clearRanges.clear();
        setRanges.clear();

        bitmap.Divide(6, 26, &clearRanges, &setRanges);
        ASSERT_EQ(2, clearRanges.size());
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(10, setRanges[0].beginIndex);
        ASSERT_EQ(18, setRanges[0].endIndex);
        ASSERT_EQ(26, setRanges[1].beginIndex);
        ASSERT_EQ(26, setRanges[1].endIndex);
        ASSERT_EQ(6, clearRanges[0].beginIndex);
        ASSERT_EQ(9, clearRanges[0].endIndex);
        ASSERT_EQ(19, clearRanges[1].beginIndex);
        ASSERT_EQ(25, clearRanges[1].endIndex);
        clearRanges.clear();
        setRanges.clear();

        bitmap.Divide(18, 22, &clearRanges, nullptr);
        ASSERT_EQ(1, clearRanges.size());
        ASSERT_EQ(19, clearRanges[0].beginIndex);
        ASSERT_EQ(22, clearRanges[0].endIndex);
        clearRanges.clear();

        bitmap.Divide(18, 26, nullptr, &setRanges);
        ASSERT_EQ(2, setRanges.size());
        ASSERT_EQ(18, setRanges[0].beginIndex);
        ASSERT_EQ(18, setRanges[0].endIndex);
        ASSERT_EQ(26, setRanges[1].beginIndex);
        ASSERT_EQ(26, setRanges[1].endIndex);
        setRanges.clear();

        bitmap.Divide(15, 5, &clearRanges, &setRanges);
        ASSERT_EQ(0, clearRanges.size());
        ASSERT_EQ(0, setRanges.size());
    }
}

}  // namespace common
}  // namespace curve
