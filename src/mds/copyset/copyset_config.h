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
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 */

#ifndef SRC_MDS_COPYSET_COPYSET_CONFIG_H_
#define SRC_MDS_COPYSET_COPYSET_CONFIG_H_

namespace curve {
namespace mds {
namespace copyset {

/**
 * @brief copyset module configuration
 */
struct CopysetOption {
    // retry times of copyset creation
    int copysetRetryTimes;
    // variance of scatterWidth
    double scatterWidthVariance;
    // standard deviation of scatterWidth
    double scatterWidthStandardDevation;
    // range of scatterWidth
    double scatterWidthRange;
    // floating percentage of scatterwidth
    double scatterWidthFloatingPercentage;

    CopysetOption()
    : copysetRetryTimes(1),
      scatterWidthVariance(0.0),
      scatterWidthStandardDevation(0.0),
      scatterWidthRange(0.0),
      scatterWidthFloatingPercentage(0.0) {}
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_COPYSET_COPYSET_CONFIG_H_
