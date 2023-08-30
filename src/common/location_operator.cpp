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

#include "src/common/location_operator.h"

namespace curve {
namespace common {

std::string LocationOperator::GenerateS3Location(
    const std::string& objectName) {
    std::string location(objectName);
    location.append(kOriginTypeSeprator).append(S3_TYPE);
    return location;
}

std::string LocationOperator::GenerateCurveLocation(
    const std::string& fileName, off_t offset) {
    std::string location(fileName);
    location.append(kOriginPathSeprator)
            .append(std::to_string(offset))
            .append(kOriginTypeSeprator)
            .append(CURVE_TYPE);
    return location;
}

OriginType LocationOperator::ParseLocation(
    const std::string& location, std::string* originPath) {
    //Found the last '@', cannot simply use SplitString
    //Because it cannot be guaranteed that OriginPath does not contain '@'
    std::string::size_type pos =
        location.find_last_of(kOriginTypeSeprator);
    if (std::string::npos == pos) {
        return OriginType::InvalidOrigin;
    }

    if (originPath != nullptr) {
        *originPath = location.substr(0, pos);
    }
    std::string typeStr = location.substr(pos + 1);

    OriginType type = OriginType::InvalidOrigin;
    if (typeStr.compare(CURVE_TYPE) == 0) {
        type = OriginType::CurveOrigin;
    } else if (typeStr.compare(S3_TYPE) == 0) {
        type = OriginType::S3Origin;
    }

    return type;
}

bool LocationOperator::ParseCurveChunkPath(
     const std::string& originPath, std::string* fileName, off_t* offset) {
    std::string::size_type pos =
        originPath.find_last_of(kOriginPathSeprator);
    if (std::string::npos == pos) {
        return false;
    }

    std::string file = originPath.substr(0, pos);
    std::string offStr = originPath.substr(pos + 1);
    if (file.empty() || offStr.empty())
        return false;

    if (fileName != nullptr) {
        *fileName = file;
    }

    if (offset != nullptr) {
        *offset = std::stoull(offStr);
    }

    return true;
}

}  // namespace common
}  // namespace curve
