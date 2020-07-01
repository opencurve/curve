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
 * Created Date: Monday March 25th 2019
 * Author: yangyaokai
 */

#ifndef SRC_COMMON_LOCATION_OPERATOR_H_
#define SRC_COMMON_LOCATION_OPERATOR_H_

#include <string>
#include <vector>

namespace curve {
namespace common {

const char CURVE_TYPE[] = "cs";
const char S3_TYPE[] = "s3";
const char kOriginTypeSeprator[] = "@";
const char kOriginPathSeprator[] = ":";

enum class OriginType {
    S3Origin = 0,
    CurveOrigin = 1,
    InvalidOrigin = 2,
};

class LocationOperator {
 public:
    /**
     * 生成s3的location
     * location格式:${objectname}@s3
     * @param objectName:s3上object的名称
     * @return:生成的location
     */
    static std::string GenerateS3Location(const std::string& objectName);
    /**
     * 生成curve的location
     * location格式:${filename}:${offset}@cs
     */
    static std::string GenerateCurveLocation(const std::string& fileName,
                                             off_t offset);
    /**
     * 解析数据源的位置信息
     * location格式:
     * s3示例：${objectname}@s3
     * curve示例：${filename}:${offset}@cs
     *
     * @param location[in]:数据源的位置，其格式为originPath@originType
     * @param originPath[out]:表示数据源在源端的路径
     * @return:返回OriginType，表示源数据的源端类型是s3还是curve
     *         如果路径格式不正确或者originType无法识别，则返回InvalidOrigin
     */
    static OriginType ParseLocation(const std::string& location,
                                    std::string* originPath);

    /**
     * 解析curvefs的originPath
     * 格式:${filename}:${offset}
     * @param originPath[in]:数据源在curvefs上的路径
     * @param fileName[out]:数据源所属文件名
     * @param offset[out]:数据源在文件中的偏移
     * @return: 解析成功返回true，失败返回false
     */
    static bool ParseCurveChunkPath(const std::string& originPath,
                                    std::string* fileName,
                                    off_t* offset);
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_LOCATION_OPERATOR_H_
