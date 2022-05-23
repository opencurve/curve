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
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common"
)

func TranslateFsType(fsType string) (common.FSType, *cmderror.CmdError) {
	fs := strings.ToUpper("TYPE_" + fsType)
	value := common.FSType_value[fs]
	var retErr cmderror.CmdError
	if value == 0 {
		retErr = *cmderror.ErrUnknownFsType()
		retErr.Format(fsType)
	}
	return common.FSType(value), &retErr
}

func TranslateBitmapLocation(bitmapLocation string) (common.BitmapLocation, *cmderror.CmdError) {
	value := common.BitmapLocation_value[bitmapLocation]
	var retErr cmderror.CmdError
	if value == 0 {
		retErr = *cmderror.ErrUnknownBitmapLocation()
		retErr.Format(bitmapLocation)
	}
	return common.BitmapLocation(value), &retErr
}

func GetCopysetKey(poolid uint64, copysetid uint64) uint64 {
	return (poolid << 32) | copysetid
}

func SplitPeerToAddr(peer string) (string, *cmderror.CmdError) {
	items := strings.Split(peer, ":")
	if len(items) != 3 {
		err := cmderror.ErrSplitPeer()
		return "", err
	}
	return items[0] + ":" + items[1], cmderror.ErrSuccess()
}
