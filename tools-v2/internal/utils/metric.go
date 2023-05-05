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

import (
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
)

const (
	kVars                    = "/vars/"
	kLogicalPoolMetricPrefix = "topology_metric_logicalPool_"
	kSechduleOpMetricpPrefix = "mds_scheduler_metric_"
)

func GetPoolLogicalCapacitySubUri(poolName string) string {
	return kVars + ToUnderscoredName(kLogicalPoolMetricPrefix+poolName+"_logicalCapacity")
}

func GetPoolLogicalAllocSubUri(poolName string) string {
	return kVars + ToUnderscoredName(kLogicalPoolMetricPrefix+poolName+"_logicalAlloc")
}

func GetPoolTotalChunkSizeName(poolName string) string {
	return kVars + ToUnderscoredName(kLogicalPoolMetricPrefix+poolName+"_chunkSizeTotalBytes")
}

func GetPoolUsedChunkSizeName(poolName string) string {
	return kVars + ToUnderscoredName(kLogicalPoolMetricPrefix+poolName+"_chunkSizeUsedBytes")
}

type CheckOperatorType int32

const (
	CheckOperatorTypeTotal    CheckOperatorType = 1
	CheckOperatorTypeChange   CheckOperatorType = 2
	CheckOperatorTypeAdd      CheckOperatorType = 3
	CheckOperatorTypeRemove   CheckOperatorType = 4
	CheckOperatorTypeTransfer CheckOperatorType = 5
)

var (
	CheckOperatorName_value = map[string]int32{
		"operator":        1,
		"change_peer":     2,
		"add_peer":        3,
		"remove_peer":     4,
		"transfer_leader": 5,
	}
)

func SupportOpName(opName string) (CheckOperatorType, *cmderror.CmdError) {
	op, ok := CheckOperatorName_value[opName]
	if !ok {
		retErr := cmderror.ErrBsOpNameNotSupport()
		retErr.Format(opName)
		return 0, retErr
	}
	return CheckOperatorType(op), cmderror.Success()
}

func GetDefaultCheckTime(opType CheckOperatorType) time.Duration {
	var checkTime time.Duration
	switch opType {
	case CheckOperatorTypeTotal, CheckOperatorTypeTransfer:
		checkTime = 30 * time.Second
	case CheckOperatorTypeChange, CheckOperatorTypeAdd, CheckOperatorTypeRemove:
		checkTime = 5 * time.Second
	}
	return checkTime
}

func GetOpNumSubUri(opName string) string {
	return kVars + ToUnderscoredName(kSechduleOpMetricpPrefix+opName+"_num")
}
