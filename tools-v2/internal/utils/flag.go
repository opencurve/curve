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
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/spf13/pflag"
)

func IsAligned(value uint64, alignment uint64) bool {
	return value&(alignment-1) == 0
}

func AvailableValueStr(flag *pflag.Flag, cmdtype cmdType) string {
	switch cmdtype {
	case BsCmd:
		return config.BsAvailableValueStr(flag.Name)
	}
	return ""
}
