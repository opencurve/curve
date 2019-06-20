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

    /**
     * @brief 验证copysets列表是否满足CopysetOption中配置的方差等度量指标
     *
     * @param copysets copysets列表
     *
     * @retval true 成功
     * @retval false 失败
     */
    bool Validate(const std::vector<Copyset> &copysets) const;

    /**
     * @brief 验证copysets的平均scatterWidth 是否满足目标scatterWidth
     *
     * @param scatterWidth 目标scatterWidth
     * @param scatterWidthOut 实际平均scatterWidth
     * @param copysets copyset列表
     *
     * @retval true 成功
     * @retval false 失败
     */
    bool ValidateScatterWidth(uint32_t scatterWidth,
        uint32_t *scatterWidthOut,
        const std::vector<Copyset> &copysets) const;

    /**
     * @brief 计算copysets的scatterWidth map
     *
     * @param copysets copysets列表
     * @param scatterWidthMap chunkserver id -> scatterWidth 的map
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
      * @brief 计算均值
      *
      * @param values 值列表
      *
      * @return 均值
      */
     static double CalcAverage(const std::vector<double> &values);
     /**
      * @brief 计算方差
      *
      * @param values 值列表
      * @param average 均值
      *
      * @return 方差
      */
     static double CalcVariance(const std::vector<double> &values,
            double average);
     /**
      * @brief 计算标准差
      *
      * @param variance 方差
      *
      * @return 标准差
      */
     static double CalcStandardDevation(double variance);
     /**
      * @brief 计算标准差
      *
      * @param values 值列表
      * @param average 均值
      *
      * @return 标准差
      */
     static double CalcStandardDevation(const std::vector<double> &values,
            double average);
     /**
      * @brief 计算极差
      *
      * @param values 值列表
      * @param minValue 最小值
      * @param maxValue 最大值
      *
      * @return 极差
      */
     static double CalcRange(const std::vector<double> &values,
            double *minValue,
            double *maxValue);
};

}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_COPYSET_COPYSET_VALIDATION_H_
