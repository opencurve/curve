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
 * Created Date: 2022-08-10
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

import (
	"path"
	"path/filepath"
	"strings"

	"github.com/cilium/cilium/pkg/mountinfo"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
)

const (
	CURVEFS_MOUNTPOINT_FSTYPE = "fuse"
)

func GetCurveFSMountPoints() ([]*mountinfo.MountInfo, *cmderror.CmdError) {
	mountpoints, err := mountinfo.GetMountInfo()
	if err != nil {
		errMountpoint := cmderror.ErrGetMountpoint()
		errMountpoint.Format(err.Error())
		return nil, errMountpoint
	}
	retMoutpoints := make([]*mountinfo.MountInfo, 0)
	for _, m := range mountpoints {
		if m.FilesystemType == CURVEFS_MOUNTPOINT_FSTYPE {
			// check if the mountpoint is a curvefs mountpoint
			retMoutpoints = append(retMoutpoints, m)
		}
	}
	return retMoutpoints, cmderror.ErrSuccess()
}

// make sure path' abs path start with mountpoint.MountPoint
func Path2CurvefsPath(path string, mountpoint *mountinfo.MountInfo) string {
	path, _ = filepath.Abs(path)
	mountPoint := mountpoint.MountPoint
	root := mountpoint.Root
	curvefsPath, _ := filepath.Abs(strings.Replace(path, mountPoint, root, 1))
	return curvefsPath
}

func CurvefsPath2ClientPath(curvefsPath string, mountpointInfo *mountinfo.MountInfo) string {
	curvefsPath, _ = filepath.Abs(curvefsPath)
	mountPoint := mountpointInfo.MountPoint
	rootInCurvefs := mountpointInfo.Root
	filename := strings.Replace(curvefsPath, rootInCurvefs, "", 1)

	clientPath := path.Join(mountPoint, filename)
	return clientPath
}
