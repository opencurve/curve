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
     * Generate location for s3
     * location format: ${objectname}@s3
     * @param objectName: The name of the object on s3
     * @return: Generated location
     */
    static std::string GenerateS3Location(const std::string& objectName);
    /**
     * Generate the location of the curve
     * location format: ${filename}:${offset}@cs
     */
    static std::string GenerateCurveLocation(const std::string& fileName,
                                             off_t offset);
    /**
     * Parsing the location information of data sources
     * location format:
     * example of s3: ${objectname}@s3
     * curve example: ${filename}:${offset}@cs
     *
     * @param location[in]: The location of the data source, in the format originPath@originType
     * @param originPath[out]: represents the path of the data source on the source side
     * @return: Returns OriginType, indicating whether the source side type of the source data is s3 or curve
     *          If the path format is incorrect or the originType is not recognized, InvalidOrigin is returned
     */
    static OriginType ParseLocation(const std::string& location,
                                    std::string* originPath);

    /**
     * Parsing the originPath of curves
     * Format: ${filename}:${offset}
     * @param originPath[in]: The path of the data source on curves
     * @param fileName[out]: The file name to which the data source belongs
     * @param offset[out]: The offset of the data source in the file
     * @return: Successful parsing returns true, while failure returns false
     */
    static bool ParseCurveChunkPath(const std::string& originPath,
                                    std::string* fileName,
                                    off_t* offset);
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_LOCATION_OPERATOR_H_
