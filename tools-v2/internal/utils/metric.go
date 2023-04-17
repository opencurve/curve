/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: CurveCli
 * Created Date: 2022-09-02
 * Author: chengyi (Cyber-SiKu)
 */
package cobrautil

const (
	kVars                    = "/vars/"
	kLogicalPoolMetricPrefix = "topology_metric_logicalPool_"
)

func GetPoolLogicalCapacitySubUri(poolName string) string {
	return kVars + ToUnderscoredName(kLogicalPoolMetricPrefix + poolName + "_logicalCapacity")
}

func GetPoolLogicalAllocSubUri(poolName string) string {
	return kVars+ToUnderscoredName(kLogicalPoolMetricPrefix + poolName + "_logicalAlloc")
}

func GetPoolTotalChunkSizeName(poolName string) string {
	return kVars+ToUnderscoredName(kLogicalPoolMetricPrefix + poolName + "_chunkSizeTotalBytes")
}

func GetPoolUsedChunkSizeName(poolName string) string {
	return kVars+ToUnderscoredName(kLogicalPoolMetricPrefix + poolName + "_chunkSizeUsedBytes")
}
