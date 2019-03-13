/*
 * Project: curve
 * Created Date: Friday April 12th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
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
