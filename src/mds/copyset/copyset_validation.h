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

#ifndef SRC_MDS_COPYSET_COPYSET_VALIDATION_H_
#define SRC_MDS_COPYSET_COPYSET_VALIDATION_H_

#include <vector>
#include <map>
#include "src/mds/copyset/copyset_structure.h"
#include "src/mds/copyset/copyset_config.h"

namespace curve {
namespace mds {
namespace copyset {

using ::curve::mds::topology::ChunkServerIdType;

class CopysetValidation{
 public:
    explicit CopysetValidation(const CopysetOption &option)
    : option_(option) {}

    /**
     * @brief validate whether the copyset list satisfy the metrics of 
     *        CopysetOption
     *
     * @param copysets copysets list
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool Validate(const std::vector<Copyset> &copysets) const;

    /**
     * @brief validate whether average scatter width of copyset satisfy 
     *        target scatterwidth
     *
     * @param scatterWidth target scatterWidth
     * @param scatterWidthOut actual average scatterWidth
     * @param copysets copyset list
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool ValidateScatterWidth(uint32_t scatterWidth,
        uint32_t *scatterWidthOut,
        const std::vector<Copyset> &copysets) const;

    /**
     * @brief calculate scatter width map of copyset
     *
     * @param copysets copysets list
     * @param scatterWidthMap map<chunkserver id, scatterWidth>
     */
    void CalcScatterWidth(
        const std::vector<Copyset> &copysets,
        std::map<ChunkServerIdType, uint32_t> *scatterWidthMap) const;

 private:
    CopysetOption option_;
};

class StatisticsTools {
 public:
     /**
      * @brief calculate average number
      *
      * @param values value list
      *
      * @return average value
      */
     static double CalcAverage(const std::vector<double> &values);
     /**
      * @brief calculate variance
      *
      * @param values value list
      * @param average average value
      *
      * @return variance
      */
     static double CalcVariance(const std::vector<double> &values,
            double average);
     /**
      * @brief calculate standard deviation
      *
      * @param variance variance value
      *
      * @return standard deviation
      */
     static double CalcStandardDevation(double variance);
     /**
      * @brief calculate standard deviation
      *
      * @param values value list
      * @param average average value
      *
      * @return standard deviation
      */
     static double CalcStandardDevation(const std::vector<double> &values,
            double average);
     /**
      * @brief calculate range
      *
      * @param values value list
      * @param minValue minimum value
      * @param maxValue maximum value
      *
      * @return range value
      */
     static double CalcRange(const std::vector<double> &values,
            double *minValue,
            double *maxValue);
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_COPYSET_COPYSET_VALIDATION_H_
