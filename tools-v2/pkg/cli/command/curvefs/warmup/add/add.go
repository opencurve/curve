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

package add

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	mountinfo "github.com/cilium/cilium/pkg/mountinfo"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/warmup/query"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

const (
	addExample = `$ curve fs warmup add --filelist /mnt/warmup/0809.list # warmup the file(dir) saved in /mnt/warmup/0809.list
$ curve fs warmup add /mnt/warmup # warmup all files in /mnt/warmup`
)

const (
	CURVEFS_WARMUP_OP_XATTR      = "curvefs.warmup.op"
	CURVEFS_WARMUP_OP_ADD_SINGLE = "add\nsingle\n%s\n%s"
	CURVEFS_WARMUP_OP_ADD_LIST   = "add\nlist\n%s\n%s"
)

var STORAGE_TYPE = map[string]string{
	"disk": "disk",
	"mem":  "kvclient",
}

type AddCommand struct {
	basecmd.FinalCurveCmd
	Mountpoint   *mountinfo.MountInfo
	Path         string // path in user system
	CurvefsPath  string // path in curvefs
	Single       bool   // warmup a single file or directory
	StorageType  string // warmup storage type
	ConvertFails []string
}

var _ basecmd.FinalCurveCmdFunc = (*AddCommand)(nil) // check interface

func NewAddWarmupCommand() *AddCommand {
	aCmd := &AddCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "add",
			Short:   "tell client to warmup files(directories) to local",
			Example: addExample,
		},
	}
	basecmd.NewFinalCurveCli(&aCmd.FinalCurveCmd, aCmd)
	return aCmd
}

func NewAddCommand() *cobra.Command {
	return NewAddWarmupCommand().Cmd
}

func (aCmd *AddCommand) AddFlags() {
	config.AddFileListOptionFlag(aCmd.Cmd)
	config.AddDaemonOptionPFlag(aCmd.Cmd)
	config.AddStorageOptionFlag(aCmd.Cmd)
}

func (aCmd *AddCommand) Init(cmd *cobra.Command, args []string) error {
	// check has curvefs mountpoint
	mountpoints, err := cobrautil.GetCurveFSMountPoints()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	} else if len(mountpoints) == 0 {
		return errors.New("no curvefs mountpoint found")
	}

	// check args
	aCmd.Single = false
	fileList := config.GetFileListOptionFlag(aCmd.Cmd)
	if fileList == "" && len(args) == 0 {
		cmd.SilenceUsage = false
		return fmt.Errorf("no --filelist or file(dir) specified")
	} else if fileList != "" {
		aCmd.Path = fileList
	} else {
		aCmd.Path = args[0]
		aCmd.Single = true
	}

	// check file is exist
	info, errStat := os.Stat(aCmd.Path)
	if errStat != nil {
		if os.IsNotExist(errStat) {
			return fmt.Errorf("[%s]: no such file or directory", aCmd.Path)
		} else {
			return fmt.Errorf("stat [%s] fail: %s", aCmd.Path, errStat.Error())
		}
	} else if !aCmd.Single && info.IsDir() {
		// --filelist must be a file
		return fmt.Errorf("[%s]: must be a file", aCmd.Path)
	}

	aCmd.Mountpoint = nil
	for _, mountpoint := range mountpoints {
		absPath, _ := filepath.Abs(aCmd.Path)
		rel, err := filepath.Rel(mountpoint.MountPoint, absPath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			// found the mountpoint
			if aCmd.Mountpoint == nil ||
				len(aCmd.Mountpoint.MountPoint) < len(mountpoint.MountPoint) {
				// Prevent the curvefs directory from being mounted under the curvefs directory
				// /a/b/c:
				// test-1 mount in /a
				// test-1 mount in /a/b
				// warmup /a/b/c.
				aCmd.Mountpoint = mountpoint
				aCmd.CurvefsPath = cobrautil.Path2CurvefsPath(aCmd.Path, mountpoint)
			}
		}
	}
	if aCmd.Mountpoint == nil {
		return fmt.Errorf("[%s] is not saved in curvefs", aCmd.Path)
	}

	// check storage type
	aCmd.StorageType = STORAGE_TYPE[config.GetStorageFlag(aCmd.Cmd)]
	if aCmd.StorageType == "" {
		return fmt.Errorf("[%s] is not support storage type", aCmd.StorageType)
	}

	return nil
}

func (aCmd *AddCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&aCmd.FinalCurveCmd, aCmd)
}

func (aCmd *AddCommand) convertFilelist() *cmderror.CmdError {
	data, err := ioutil.ReadFile(aCmd.Path)
	if err != nil {
		readErr := cmderror.ErrReadFile()
		readErr.Format(aCmd.Path, err.Error())
		return readErr
	}

	lines := strings.Split(string(data), "\n")
	validPath := ""
	for _, line := range lines {
		rel, err := filepath.Rel(aCmd.Mountpoint.MountPoint, line)
		if err == nil && !strings.HasPrefix(rel, "..") {
			// convert to curvefs path
			curvefsAbspath := cobrautil.Path2CurvefsPath(line, aCmd.Mountpoint)
			validPath += (curvefsAbspath + "\n")
		} else {
			convertFail := fmt.Sprintf("[%s] is not saved in curvefs", line)
			aCmd.ConvertFails = append(aCmd.ConvertFails, convertFail)
		}
	}
	if err = ioutil.WriteFile(aCmd.Path, []byte(validPath), 0644); err != nil {
		writeErr := cmderror.ErrWriteFile()
		writeErr.Format(aCmd.Path, err.Error())
	}
	return cmderror.ErrSuccess()
}

func (aCmd *AddCommand) RunCommand(cmd *cobra.Command, args []string) error {
	xattr := CURVEFS_WARMUP_OP_ADD_SINGLE
	if !aCmd.Single {
		convertErr := aCmd.convertFilelist()
		if convertErr.TypeCode() != cmderror.CODE_SUCCESS {
			return convertErr.ToError()
		}
		xattr = CURVEFS_WARMUP_OP_ADD_LIST
	}
	value := fmt.Sprintf(xattr, aCmd.CurvefsPath, aCmd.StorageType)
	err := unix.Setxattr(aCmd.Path, CURVEFS_WARMUP_OP_XATTR, []byte(value), 0)
	if err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
		return fmt.Errorf("filesystem does not support extended attributes")
	} else if err != nil {
		setErr := cmderror.ErrSetxattr()
		setErr.Format(CURVEFS_WARMUP_OP_XATTR, err.Error())
		return setErr.ToError()
	}
	if config.GetDaemonFlag(aCmd.Cmd) {
		query.GetWarmupProgress(aCmd.Cmd, aCmd.Path)
	}
	return nil
}

func (aCmd *AddCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&aCmd.FinalCurveCmd)
}
