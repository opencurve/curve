/*
 * Project: curve
 * Created Date: Mon May 13 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>

#include "src/mds/copyset/copyset_manager.h"


namespace curve {
namespace mds {
namespace copyset {


TEST(TestCopysetValidation, testValidateScatterWidthSuccess) {
    CopysetOption option;
    CopysetValidation validator(option);

    int scatterWidth = 4;
    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);

    ASSERT_TRUE(validator.ValidateScatterWidth(
        scatterWidth, copysets));
}

TEST(TestCopysetValidation, testValidateScatterWidthFail) {
    CopysetOption option;
    CopysetValidation validator(option);

    int scatterWidth = 5;
    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);

    ASSERT_FALSE(validator.ValidateScatterWidth(
        scatterWidth, copysets));
}


TEST(TestCopysetValidation, CalcScatterWidthSuccess) {
    CopysetOption option;
    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);

    std::map<ChunkServerIdType, int> scatterWidthMap;
    validator.CalcScatterWidth(copysets, &scatterWidthMap);

    ASSERT_EQ(4, scatterWidthMap.size());
    ASSERT_EQ(4, scatterWidthMap[1]);
    ASSERT_EQ(4, scatterWidthMap[2]);
    ASSERT_EQ(4, scatterWidthMap[3]);
    ASSERT_EQ(4, scatterWidthMap[4]);
}

TEST(TestCopysetValidation, CalcValidateSuccess) {
    CopysetOption option;
    option.scatterWidthVariance = 1;
    option.scatterWidthStandardDevation = 1;
    option.scatterWidthRange = 1;

    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);

    ASSERT_TRUE(validator.Validate(copysets));
}

TEST(TestCopysetValidation, CalcValidateFailOnVariance) {
    CopysetOption option;
    option.scatterWidthVariance = 0.0001;
    option.scatterWidthStandardDevation = 0.0000001;
    option.scatterWidthRange = 1;

    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4, cs5;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    cs5.replicas = {1, 2, 5};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);
    copysets.push_back(cs5);

    ASSERT_FALSE(validator.Validate(copysets));
}

TEST(TestCopysetValidation, CalcValidateFailOnStandardDevtion) {
    CopysetOption option;
    option.scatterWidthVariance = 1;
    option.scatterWidthStandardDevation = 0.0000001;
    option.scatterWidthRange = 1;

    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4, cs5;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {4, 2, 3};
    cs5.replicas = {1, 2, 5};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);
    copysets.push_back(cs5);

    ASSERT_FALSE(validator.Validate(copysets));
}

TEST(TestCopysetValidation, CalcValidateFailOnRange) {
    CopysetOption option;
    option.scatterWidthVariance = 1;
    option.scatterWidthStandardDevation = 1;
    option.scatterWidthRange = 1;

    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4, cs5;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {6, 2, 3};
    cs5.replicas = {1, 2, 5};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);
    copysets.push_back(cs5);

    ASSERT_FALSE(validator.Validate(copysets));
}

TEST(TestCopysetValidation, CalcValidateFailOnFloatingPercentage) {
    CopysetOption option;
    option.scatterWidthFloatingPercentage = 0.1;

    CopysetValidation validator(option);

    std::vector<Copyset> copysets;
    Copyset cs1, cs2, cs3, cs4, cs5;
    cs1.replicas = {1, 2, 3};
    cs2.replicas = {1, 2, 4};
    cs3.replicas = {1, 4, 3};
    cs4.replicas = {6, 2, 3};
    cs5.replicas = {1, 2, 5};
    copysets.push_back(cs1);
    copysets.push_back(cs2);
    copysets.push_back(cs3);
    copysets.push_back(cs4);
    copysets.push_back(cs5);

    ASSERT_FALSE(validator.Validate(copysets));
}

TEST(TestStatisticsTools, CalcAverageSuccess) {
    std::vector<double> values{1.0, 2.0, 3.0, 4.0};

    ASSERT_EQ(2.5, StatisticsTools::CalcAverage(values));
}

TEST(TestStatisticsTools, CalcAverageEmpty) {
    std::vector<double> values;
    ASSERT_EQ(0, StatisticsTools::CalcAverage(values));
}

TEST(TestStatisticsTools, CalcVarianceSuccess) {
    std::vector<double> values{1.0, 2.0, 3.0, 4.0};
    ASSERT_EQ(1.25, StatisticsTools::CalcVariance(values, 2.5));
}

TEST(TestStatisticsTools, CalcVarianceEmpty) {
    std::vector<double> values;
    ASSERT_EQ(0, StatisticsTools::CalcVariance(values, 0));
}

TEST(TestStatisticsTools, CalcStandardDevationSuccess) {
    ASSERT_EQ(2, StatisticsTools::CalcStandardDevation(4));
}

TEST(TestStatisticsTools, CalcStandardDevationNegativeNum) {
    ASSERT_EQ(0, StatisticsTools::CalcStandardDevation(-1));
}

TEST(TestStatisticsTools, CalcRangeSuccess) {
    std::vector<double> values{1.0, -2.0, -3.0, 4.0};
    double minValue = 0;
    double maxValue = 0;
    ASSERT_EQ(7, StatisticsTools::CalcRange(values, &minValue, &maxValue));
    ASSERT_EQ(-3.0, minValue);
    ASSERT_EQ(4.0, maxValue);
}

TEST(TestStatisticsTools, CalcRangeEmpty) {
    std::vector<double> values;
    double minValue = 0;
    double maxValue = 0;
    ASSERT_EQ(0, StatisticsTools::CalcRange(values, &minValue, &maxValue));
    ASSERT_EQ(0, minValue);
    ASSERT_EQ(0, maxValue);
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve
