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
 * Created Date: Tuesday November 27th 2018
 * Author: yangyaokai
 */

#ifndef SRC_COMMON_BITMAP_H_
#define SRC_COMMON_BITMAP_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace curve
{
    namespace common
    {

        using std::vector;

        const int BITMAP_UNIT_SIZE = 8;
        const int ALIGN_FACTOR = 3; // 2 ^ ALIGN_FACTOR = BITMAP_UNIT_SIZE

        /**
         * Represents a continuous region in a bitmap, which is a closed interval
         */
        struct BitRange
        {
            // Index of the starting position of a continuous region in Bitmap
            uint32_t beginIndex;
            // Index of the end position of a continuous region in Bitmap
            uint32_t endIndex;
        };

        std::string BitRangeVecToString(const std::vector<BitRange> &ranges);

        class Bitmap
        {
        public:
            /**
             * Constructor when creating a new bitmap
             * @param bits: The number of bits to construct the bitmap
             */
            explicit Bitmap(uint32_t bits);
            /**
             * Constructor when initializing from an existing snapshot file
             * The constructor will create a new bitmap internally, and then use the
             * bitmap memcpy in the parameters
             * @param bits: Bitmap bits
             * @param bitmap: An externally provided bitmap for initialization
             */
            explicit Bitmap(uint32_t bits, const char *bitmap);

            // Construct from a given bitmap, if transfer is false, allocate enough
            // memory and copy the given bitmap, otherwise, just store the pointer
            Bitmap(uint32_t bits, char *bitmap, bool transfer = false);

            ~Bitmap();

            /**
             * Copy construction, using deep copy
             * @param bitmap: Copy content from this object
             */
            Bitmap(const Bitmap &bitmap);
            /**
             *Assignment function, using deep copy
             * @param bitmap: Copy content from this object
             * @reutrn: Returns the copied object reference
             */
            Bitmap &operator=(const Bitmap &bitmap);

            Bitmap(Bitmap &&other) noexcept;
            Bitmap &operator=(Bitmap &&other) noexcept;

            /**
             * Compare whether two bitmaps are the same
             * @param bitmap: Bitmap to be compared
             * @return: Returns true if the same, false if different
             */
            bool operator==(const Bitmap &bitmap) const;
            /**
             * Compare whether two bitmaps are different
             * @param bitmap: Bitmap to be compared
             * @return: Returns true if different, false if the same
             */
            bool operator!=(const Bitmap &bitmap) const;
            /**
             * Place all positions 1
             */
            void Set();
            /**
             * Specify position 1
             * @param index: Refers to the location of the positioning
             */
            void Set(uint32_t index);
            /**
             * Set the position of the specified range to 1
             * @param startIndex: The starting position of the range, including this
             * position
             * @param endIndex: The end position of the range, including this position
             */
            void Set(uint32_t startIndex, uint32_t endIndex);
            /**
             * Move all positions to 0
             */
            void Clear();
            /**
             * Will specify position 0
             * @param index: Refers to the location of the positioning
             */
            void Clear(uint32_t index);
            /**
             * Set the position of the specified range to 0
             * @param startIndex: The starting position of the range, including this
             * position
             * @param endIndex: The end position of the range, including this position
             */
            void Clear(uint32_t startIndex, uint32_t endIndex);
            /**
             * Obtain the status of the specified position bit
             * @param index: Refers to the location of the positioning
             * @return: true indicates that the current bit status is 1, while false
             * indicates that it is 0
             */
            bool Test(uint32_t index) const;
            /**
             * Obtain the specified position and the position after which the first bit
             * is 1
             * @param index: Refers to the location of the positioning, including this
             * location
             * @return: The position where the first bit is 1. If it does not exist,
             * return NO_POS
             */
            uint32_t NextSetBit(uint32_t index) const;
            /**
             * Gets the position where the first bit between the specified start
             * position and end position is 1
             * @param startIndex: The starting position, including this position
             * @param endIndex: End position, including this position
             * @return: The position where the first bit is 1. If it does not exist
             * within the specified range, return NO_POS
             */
            uint32_t NextSetBit(uint32_t startIndex, uint32_t endIndex) const;
            /**
             * Obtain the specified position and the position after which the first bit
             * is 0
             * @param index: Refers to the location of the positioning, including this
             * location
             * @return: The position where the first bit is 0. If it does not exist,
             * return NO_POS
             */
            uint32_t NextClearBit(uint32_t index) const;
            /**
             * Gets the position where the first bit between the specified start
             * position and end position is 0
             * @param startIndex: The starting position, including this position
             * @param endIndex: End position, including this position
             * @return: The position where the first bit is 0. If it does not exist
             * within the specified range, return NO_POS
             */
            uint32_t NextClearBit(uint32_t startIndex, uint32_t endIndex) const;
            /**
             * Divide the designated area of the bitmap into several continuous areas
             * based on bit states, with consistent bit states within the continuous
             * areas For example, 00011100 will be divided into three regions: [0,2],
             * [3,5], [6,7]
             * @param startIndex: The starting index of the specified region
             * @param endIndex: The end index of the specified range
             * @param clearRanges: A vector that stores a continuous region with a bit
             * state of 0, which can be specified as nullptr
             * @param setRanges: A vector that stores a continuous region with a bit
             * state of 1, which can be specified as nullptr
             */
            void Divide(uint32_t startIndex, uint32_t endIndex,
                        vector<BitRange> *clearRanges,
                        vector<BitRange> *setRanges) const;
            /**
             * Bitmap's significant digits
             * @return: Returns the number of digits
             */
            uint32_t Size() const;
            /**
             * Obtain a memory pointer to Bitmap for persisting Bitmap
             * @return: Memory pointer to bitmap
             */
            const char *GetBitmap() const;

        private:
            // Bytes of bitmap
            int unitCount() const
            {
                // Same as (bits_ + BITMAP_UNIT_SIZE - 1) / BITMAP_UNIT_SIZE
                return (bits_ + BITMAP_UNIT_SIZE - 1) >> ALIGN_FACTOR;
            }
            // The offset of the bit at the specified position in its byte
            int indexOfUnit(uint32_t index) const
            {
                // Same as index / BITMAP_UNIT_SIZE
                return index >> ALIGN_FACTOR;
            }
            // Logical calculation mask value
            char mask(uint32_t index) const
            {
                int indexInUnit = index % BITMAP_UNIT_SIZE;
                char mask = 0x01 << indexInUnit;
                return mask;
            }

        public:
            // Represents a non-existent position, with a value of 0xffffffff
            static const uint32_t NO_POS;

        private:
            uint32_t bits_;
            char *bitmap_;
        };

    } // namespace common
} // namespace curve

#endif // SRC_COMMON_BITMAP_H_
