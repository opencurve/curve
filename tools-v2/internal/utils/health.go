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
 * Created Date: 2022-06-21
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

import (
	"github.com/gookit/color"
)

type ClUSTER_HEALTH_STATUS int32

const HEALTH = "health"

const (
	HEALTH_OK    ClUSTER_HEALTH_STATUS = 1
	HEALTH_WARN  ClUSTER_HEALTH_STATUS = 2
	HEALTH_ERROR ClUSTER_HEALTH_STATUS = 3
)

var (
	ClusterHealthStatus_Str = map[int32]string{
		1: "ok",
		2: "warn",
		3: "error",
	}
)

var ClusterHealthStatus_StrWithColor = map[int32]string{
	1: color.Green.Sprint(CopysetHealthStatus_Str[1]),
	2: color.Yellow.Sprint(CopysetHealthStatus_Str[2]),
	3: color.Red.Sprint(CopysetHealthStatus_Str[3]),
	4: color.Red.Sprint(CopysetHealthStatus_Str[4]),
}

func CompareHealth(a ClUSTER_HEALTH_STATUS, b ClUSTER_HEALTH_STATUS) ClUSTER_HEALTH_STATUS {
	if a > b {
		return a
	}
	return b
}
