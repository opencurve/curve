/*
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_COPYSET_COPYSET_MANAGER_H_
#define SRC_MDS_COPYSET_COPYSET_MANAGER_H_


#include <memory>
#include <vector>

#include "src/mds/copyset/copyset_policy.h"
#include "src/mds/copyset/copyset_validation.h"
#include "src/mds/copyset/copyset_config.h"

namespace curve {
namespace mds {
namespace copyset {

class CopysetManager {
 public:
    explicit CopysetManager(const CopysetOption &option)
      : option_(option) {
        validator_ = std::make_shared<CopysetValidation>(option);
    }

    ~CopysetManager() {}

    /**
     * @brief 初始化
     *
     * @param constrait copyset约束
     *
     * @return 若当前约束不支持，则返回false，否则返回true
     */
    bool Init(const CopysetConstrait &constrait);

    /**
     * @brief 生成copyset
     *
     * @detail
     * 1. 若copysetNum和scatterWidth为0，则执行失败
     * 2. 若只提供copysetNum，则根据copysetNum生成copyset
     * 3. 若同时提供copysetNum和scatterWidth,
     * 则首先通过copysetNum生成copyset，并验证scatterWidth是否满足。
     * 4. 若只提供scatterWidth， 则根据scatterWidth
     * 生成足够数量满足scatterWidth的copyset
     *
     * @param cluster 集群信息
     * @param numCopysets copyset数量
     * @param scatterWidth scatterWidth平均值
     * @param out copyset列表
     *
     * @retval true 成功
     * @retval false 失败
     */
    bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        int scatterWidth,
        std::vector<Copyset>* out);

 private:
    /**
     * @brief 根据copysetNum生成copyset
     *
     * @param cluster 集群信息
     * @param numCopysets copyset数量
     * @param out copyset列表
     *
     * @retval true 成功
     * @retval false 失败
     */
    bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        std::vector<Copyset>* out);

 private:
    CopysetOption option_;
    std::shared_ptr<CopysetPolicy> policy_;
    std::shared_ptr<CopysetValidation> validator_;
    CopysetConstrait constrait_;
};


}  // namespace copyset
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_COPYSET_COPYSET_MANAGER_H_
