/*
 * Project: curve
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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

    bool Validate(const std::vector<Copyset> &copysets) const;

    bool ValidateScatterWidth(int scatterWidth,
        const std::vector<Copyset> &copysets) const;

    void CalcScatterWidth(
        const std::vector<Copyset> &copysets,
        std::map<ChunkServerIdType, int> *scatterWidthMap) const;

 private:
    CopysetOption option_;
};

class StatisticsTools {
 public:
     static double CalcAverage(const std::vector<double> &values);
     static double CalcVariance(const std::vector<double> &values,
            double average);
     static double CalcStandardDevation(double variance);
     static double CalcStandardDevation(const std::vector<double> &values,
            double average);
     static double CalcRange(const std::vector<double> &values,
            double *minValue,
            double *maxValue);
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_COPYSET_COPYSET_VALIDATION_H_
