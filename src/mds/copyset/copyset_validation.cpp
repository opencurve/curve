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
 * Created Date: Thu May 09 2019
 * Author: xuchaojie
 */

#include "src/mds/copyset/copyset_validation.h"

#include <glog/logging.h>

#include <set>
#include <map>
#include <vector>
#include <cmath>
#include <algorithm>

namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;

bool CopysetValidation::Validate(
    const std::vector<Copyset> &copysets) const {
    std::map<ChunkServerIdType, uint32_t> scatterWidthMap;
    CalcScatterWidth(copysets, &scatterWidthMap);
    std::vector<double> scatterWidthVec;
    for (auto &pair : scatterWidthMap) {
        scatterWidthVec.push_back(pair.second);
    }

    double average = StatisticsTools::CalcAverage(scatterWidthVec);
    double variance = 0;
    if ((option_.scatterWidthVariance != 0) ||
        (option_.scatterWidthStandardDevation != 0)) {
        variance = StatisticsTools::CalcVariance(scatterWidthVec,
            average);
    }
    // check variance
    if (option_.scatterWidthVariance != 0 &&
        variance > option_.scatterWidthVariance) {
        LOG(WARNING) << "Validate copyset failed in scatterWidthVariance = "
                   << option_.scatterWidthVariance
                   << ", actual = " << variance;
        return false;
    }

    // check standard deviation
    if (option_.scatterWidthStandardDevation != 0) {
        double standardDevation =
            StatisticsTools::CalcStandardDevation(variance);
        if (standardDevation > option_.scatterWidthStandardDevation) {
            LOG(WARNING) << "Validate copyset failed in "
                       << "scatterWidthStandardDevation = "
                       << option_.scatterWidthStandardDevation
                       << ", actual = " << standardDevation;
            return false;
        }
    }

    double range = 0;
    double minValue = 0;
    double maxValue = 0;
    if ((option_.scatterWidthRange != 0) ||
        (option_.scatterWidthFloatingPercentage != 0)) {
        range = StatisticsTools::CalcRange(
            scatterWidthVec, &minValue, &maxValue);
    }

    // check range
    if ((option_.scatterWidthRange != 0) &&
        (range > option_.scatterWidthRange)) {
        LOG(WARNING) << "Validate copyset failed in "
                   << "scatterWidthRange = "
                   << option_.scatterWidthRange
                   << ", actual = " << range;
        return false;
    }
    // check floating percentage
    if (option_.scatterWidthFloatingPercentage != 0) {
        double minPercent = (average - minValue) * 100 / average;
        double maxPercent = (maxValue - average) * 100 / average;
        if ((minPercent > option_.scatterWidthFloatingPercentage) ||
            (maxPercent > option_.scatterWidthFloatingPercentage)) {
            LOG(WARNING) << "Validate copyset failed in "
                       << "scatterWidthFloatingPercentage = "
                       << option_.scatterWidthFloatingPercentage
                       << ", actual minValue = " << minValue
                       << ", maxValue = " << maxValue
                       << ", average = " << average;
            return false;
        }
    }
    return true;
}

bool CopysetValidation::ValidateScatterWidth(uint32_t scatterWidth,
    uint32_t *scatterWidthOut,
    const std::vector<Copyset> &copysets) const {
    std::map<ChunkServerIdType, uint32_t> scatterWidthMap;
    CalcScatterWidth(copysets, &scatterWidthMap);
    std::vector<double> scatterWidthVec;
    for (auto &pair : scatterWidthMap) {
        scatterWidthVec.push_back(pair.second);
    }
    double average = StatisticsTools::CalcAverage(scatterWidthVec);
    *scatterWidthOut = std::round(average);
    if (average < scatterWidth) {
        LOG(WARNING) << "ValidateScatterWidth failed"
                   << ", scatterWidth = " << scatterWidth
                   << ", current = " << average;
        return false;
    }
    return true;
}

void CopysetValidation::CalcScatterWidth(const std::vector<Copyset> &copysets,
    std::map<ChunkServerIdType, uint32_t> *scatterWidthMap) const {
    for (auto cs : copysets) {
        for (auto csId : cs.replicas) {
            scatterWidthMap->emplace(csId, 0);
        }
    }
    std::set<ChunkServerIdType> collector;
    for (auto &pair : *scatterWidthMap) {
        collector.clear();
        for (auto cs : copysets) {
            if (cs.replicas.count(pair.first) != 0) {
                collector.insert(cs.replicas.begin(), cs.replicas.end());
            }
        }
        // scatterWidth -1 is for excluding one of the copy itself (see the definition of scatter width)
        pair.second = collector.size() - 1;
    }
}

double StatisticsTools::CalcAverage(const std::vector<double> &values) {
    if (values.size() == 0) {
        return 0;
    }
    double sum = 0;
    for (auto &v : values) {
        sum += v;
    }
    return sum/values.size();
}

double StatisticsTools::CalcVariance(const std::vector<double> &values,
       double average) {
    if (values.size() == 0) {
        return 0;
    }
    double sum = 0;
    for (auto &v : values) {
        sum += (v - average) * (v - average);
    }
    return sum/values.size();
}

double StatisticsTools::CalcStandardDevation(double variance) {
    return variance > 0 ? std::sqrt(variance) : 0;
}

double StatisticsTools::CalcStandardDevation(
    const std::vector<double> &values,
    double average) {
    return CalcStandardDevation(CalcVariance(values, average));
}

double StatisticsTools::CalcRange(const std::vector<double> &values,
    double *minValue,
    double *maxValue) {
    if (values.size() == 0) {
        *minValue = 0;
        *maxValue = 0;
        return 0;
    }
    *minValue = values[0];
    *maxValue = values[0];
    for (auto &v : values) {
        *minValue = std::min(*minValue, v);
        *maxValue = std::max(*maxValue, v);
    }
    return *maxValue - *minValue;
}

}  // namespace copyset
}  // namespace mds
}  // namespace curve

