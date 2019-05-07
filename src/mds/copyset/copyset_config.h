/*
 * Project: curve
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_COPYSET_COPYSET_CONFIG_H_
#define SRC_MDS_COPYSET_COPYSET_CONFIG_H_

namespace curve {
namespace mds {
namespace copyset {

/**
 * @brief copyset模块配置项
 */
struct CopysetOption {
    // 生成copyset算法重试次数
    int copysetRetryTimes;
    // scatterWidth方差
    double scatterWidthVariance;
    // scatterWidth标准差
    double scatterWidthStandardDevation;
    // scatterWidth极差
    double scatterWidthRange;
    // 偏离均值的百分比
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
