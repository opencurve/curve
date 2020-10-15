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
 * Project:
 * Created Date: Wed Oct 10 2018
 * Author: xuchaojie
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
     * @brief initialization
     *
     * @param constrait copyset constraint
     *
     * @return return 'false' if the constraint is not supported, 
     *         otherwise 'true'.
     */
    bool Init(const CopysetConstrait &constrait);

    /**
     * @brief generate copyset
     *
     * @detail
     * 1. if copysetNum and scatterWidth are 0, execution fail
     * 2. if only copysetNum provided, generate copyset according to 
     *    copyset number
     * 3. if copysetNum and scatterWidth both provided,
     *    then we first generate copyset using copysetNum, 
     *    and verify whether scatterWidth is satisfied
     * 4. if only scatterWidth provided, generate certain amount of copysets 
     *    that will satisfy scatterWidth requirement
     *
     * @param cluster cluster information
     * @param numCopysets copyset numbers 
     * @param[in][out] scatterWidth carry in target scatter width as an input, 
     *                 and carry out actual scatter width as an output
     * @param out copyset list
     *
     * @retval true if succeeded
     * @retval false if failed
     */
    bool GenCopyset(const ClusterInfo& cluster,
        int numCopysets,
        uint32_t *scatterWidth,
        std::vector<Copyset>* out);

 private:
    /**
     * @brief generate copyset according to copysetNum
     *
     * @param cluster cluster information
     * @param numCopysets copyset number
     * @param out copyset list
     *
     * @retval true if succeeded
     * @retval false if failed
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
