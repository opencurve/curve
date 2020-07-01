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
 * Created Date: Friday April 12th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>

#include "src/common/location_operator.h"

namespace curve {
namespace common {

TEST(LocationOperatorTest, GenerateTest) {
    std::string location = LocationOperator::GenerateS3Location("test");
    ASSERT_STREQ("test@s3", location.c_str());

    location = LocationOperator::GenerateCurveLocation("test", 0);
    ASSERT_STREQ("test:0@cs", location.c_str());
}

TEST(LocationOperatorTest, GenerateCurveLocationTest) {
    std::string originPath;
    std::string location;

    location = "test";
    ASSERT_EQ(OriginType::InvalidOrigin,
              LocationOperator::ParseLocation(location, &originPath));

    location = "test@b";
    ASSERT_EQ(OriginType::InvalidOrigin,
              LocationOperator::ParseLocation(location, &originPath));

    location = "test@s3@";
    ASSERT_EQ(OriginType::InvalidOrigin,
              LocationOperator::ParseLocation(location, &originPath));

    location = "test@s3";
    ASSERT_EQ(OriginType::S3Origin,
              LocationOperator::ParseLocation(location, &originPath));
    ASSERT_STREQ(originPath.c_str(), "test");

    location = "test@cs";
    ASSERT_EQ(OriginType::CurveOrigin,
              LocationOperator::ParseLocation(location, &originPath));
    ASSERT_STREQ(originPath.c_str(), "test");

    location = "test@test@cs";
    ASSERT_EQ(OriginType::CurveOrigin,
              LocationOperator::ParseLocation(location, &originPath));
    ASSERT_STREQ(originPath.c_str(), "test@test");

    location = "test@test@cs";
    ASSERT_EQ(OriginType::CurveOrigin,
              LocationOperator::ParseLocation(location, nullptr));
}

TEST(LocationOperatorTest, ParseCurvePathTest) {
    std::string originPath;
    std::string fileName;
    off_t offset;

    originPath = "test";
    ASSERT_EQ(false,
        LocationOperator::ParseCurveChunkPath(originPath, &fileName, &offset));

    originPath = "test:";
    ASSERT_EQ(false,
        LocationOperator::ParseCurveChunkPath(originPath, &fileName, &offset));

    originPath = ":0";
    ASSERT_EQ(false,
        LocationOperator::ParseCurveChunkPath(originPath, &fileName, &offset));

    originPath = "test:0";
    ASSERT_EQ(true,
        LocationOperator::ParseCurveChunkPath(originPath, &fileName, &offset));
    ASSERT_STREQ(fileName.c_str(), "test");
    ASSERT_EQ(offset, 0);

    originPath = "test:0:0";
    ASSERT_EQ(true,
        LocationOperator::ParseCurveChunkPath(originPath, &fileName, &offset));
    ASSERT_STREQ(fileName.c_str(), "test:0");
    ASSERT_EQ(offset, 0);

    originPath = "test:0";
    ASSERT_EQ(true,
        LocationOperator::ParseCurveChunkPath(originPath, nullptr, nullptr));
}

}  // namespace common
}  // namespace curve
