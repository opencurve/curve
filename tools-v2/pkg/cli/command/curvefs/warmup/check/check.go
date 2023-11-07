/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-11-07
 * Author: chengyi (Cyber-SiKu)
 */

package check

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
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/pkg/xattr"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

const (
	checkExample = `$ curve fs warmup check --filelist /mnt/warmup/0809.list # warmup the file(dir) saved in /mnt/warmup/0809.list
$ curve fs warmup check /mnt/warmup # warmup all files in /mnt/warmup`
)

const (
	CURVEFS_WARMUP_OP_XATTR      = "curvefs.warmup.op"
	CURVEFS_WARMUP_CEHCK_XATTR      = "curvefs.warmup.check"
	CURVEFS_WARMUP_OP_CHECK_SINGLE = "check\nsingle\n%s\n%s\n%s\n%s"
	CURVEFS_WARMUP_OP_CHECK_LIST   = "check\nlist\n%s\n%s\n%s\n%s"
)

var STORAGE_TYPE = map[string]string{
	"disk": "disk",
	"mem":  "kvclient",
}

type CheckCommand struct {
	basecmd.FinalCurveCmd
	Mountpoint  *mountinfo.MountInfo
	Path        string // path in user system
	CurvefsPath string // path in curvefs
	Single      bool   // warmup a single file or directory
	StorageType string // warmup storage type
}

var _ basecmd.FinalCurveCmdFunc = (*CheckCommand)(nil) // check interface

func NewAddWarmupCommand() *CheckCommand {
	cCmd := &CheckCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "check",
			Short:   "Check if the files/dir are all in the cache",
			Example: checkExample,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func NewCheckCommand() *cobra.Command {
	return NewAddWarmupCommand().Cmd
}

func (cCmd *CheckCommand) AddFlags() {
	config.AddFileListOptionFlag(cCmd.Cmd)
	config.AddStorageOptionFlag(cCmd.Cmd)
}

func (cCmd *CheckCommand) Init(cmd *cobra.Command, args []string) error {
	// check has curvefs mountpoint
	mountpoints, err := cobrautil.GetCurveFSMountPoints()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	} else if len(mountpoints) == 0 {
		return errors.New("no curvefs mountpoint found")
	}

	// check args
	cCmd.Single = false
	fileList := config.GetFileListOptionFlag(cCmd.Cmd)
	if fileList == "" && len(args) == 0 {
		cmd.SilenceUsage = false
		return fmt.Errorf("no --filelist or file(dir) specified")
	} else if fileList != "" {
		cCmd.Path = fileList
	} else {
		cCmd.Path = args[0]
		cCmd.Single = true
	}

	// check file is exist
	info, errStat := os.Stat(cCmd.Path)
	if errStat != nil {
		if os.IsNotExist(errStat) {
			return fmt.Errorf("[%s]: no such file or directory", cCmd.Path)
		} else {
			return fmt.Errorf("stat [%s] fail: %s", cCmd.Path, errStat.Error())
		}
	} else if !cCmd.Single && info.IsDir() {
		// --filelist must be a file
		return fmt.Errorf("[%s]: must be a file", cCmd.Path)
	}

	cCmd.Mountpoint = nil
	for _, mountpoint := range mountpoints {
		absPath, _ := filepath.Abs(cCmd.Path)
		rel, err := filepath.Rel(mountpoint.MountPoint, absPath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			// found the mountpoint
			if cCmd.Mountpoint == nil ||
				len(cCmd.Mountpoint.MountPoint) < len(mountpoint.MountPoint) {
				// Prevent the curvefs directory from being mounted under the curvefs directory
				// /a/b/c:
				// test-1 mount in /a
				// test-1 mount in /a/b
				// warmup /a/b/c.
				cCmd.Mountpoint = mountpoint
				cCmd.CurvefsPath = cobrautil.Path2CurvefsPath(cCmd.Path, mountpoint)
			}
		}
	}
	if cCmd.Mountpoint == nil {
		return fmt.Errorf("[%s] is not saved in curvefs", cCmd.Path)
	}

	// check storage type
	cCmd.StorageType = STORAGE_TYPE[config.GetStorageFlag(cCmd.Cmd)]
	if cCmd.StorageType == "" {
		return fmt.Errorf("[%s] is not support storage type", cCmd.StorageType)
	}

	cCmd.SetHeader([]string{cobrautil.ROW_PATH, cobrautil.ROW_RESULT})
	return nil
}

func (cCmd *CheckCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CheckCommand) verifyFilelist() *cmderror.CmdError {
	data, err := ioutil.ReadFile(cCmd.Path)
	if err != nil {
		readErr := cmderror.ErrReadFile()
		readErr.Format(cCmd.Path, err.Error())
		return readErr
	}

	lines := strings.Split(string(data), "\n")

	verifyFailMsg := ""
	var verifyReplaceErr error
	for i, line := range lines {
		if line == "" {
			continue
		}
		rel, err := filepath.Rel(cCmd.Mountpoint.MountPoint, line)
		if err != nil || strings.HasPrefix(rel, "..") {
			verifyReplaceErr = err
			verifyFailMsg += fmt.Sprintf("line %d: [%s:%s] is not saved in curvefs\n", i + 1, cCmd.Path, line)
		}
	}
	
	if verifyReplaceErr != nil {
		verifyErr := cmderror.ErrVerifyError()
		verifyErr.Format(verifyFailMsg, verifyReplaceErr.Error())
		return verifyErr
	}
	return cmderror.ErrSuccess()
}

func (cCmd *CheckCommand) RunCommand(cmd *cobra.Command, args []string) error {
	checkAttr := CURVEFS_WARMUP_OP_CHECK_SINGLE
	if !cCmd.Single {
		verifyErr := cCmd.verifyFilelist()
		if verifyErr.TypeCode() != cmderror.CODE_SUCCESS {
			return verifyErr.ToError()
		}
		checkAttr = CURVEFS_WARMUP_OP_CHECK_LIST
	}
	values := fmt.Sprintf(checkAttr, cCmd.CurvefsPath, cCmd.StorageType, cCmd.Mountpoint.MountPoint, cCmd.Mountpoint.Root)
	err := xattr.Set(cCmd.Path, CURVEFS_WARMUP_OP_XATTR, []byte(values))
	if err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
		return fmt.Errorf("filesystem does not support extended attributes")
	} else if err != nil {
		setErr := cmderror.ErrSetxattr()
		setErr.Format(CURVEFS_WARMUP_OP_XATTR, err.Error())
		return setErr.ToError()
	}
	result, err := xattr.Get(cCmd.Path, CURVEFS_WARMUP_CEHCK_XATTR)
	if err != nil {
		return err
	}
	cCmd.TableNew.Append([]string{cCmd.Path, string(result)})
	return nil
}

func (cCmd *CheckCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
