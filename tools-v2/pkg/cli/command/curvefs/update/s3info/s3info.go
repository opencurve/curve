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
 * Created Date: 2024-01-24
 * Author: ken90242 (Ken Han)
 */

 package s3info

 import (
	 "errors"
	 "encoding/json"
	 "fmt"
	 "os"
	 "path/filepath"
	 "strings"
 
	 basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	 cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	 cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	 mountinfo "github.com/cilium/cilium/pkg/mountinfo"

	 "github.com/opencurve/curve/tools-v2/pkg/config"
	 "github.com/opencurve/curve/tools-v2/pkg/output"
	 "github.com/spf13/cobra"
	 "golang.org/x/sys/unix"
 )
 
 const (
	updateS3InfoExample = `$ curve fs update s3info /mnt/test --s3.ak=ak`
 )
 
 const (
	 CURVEFS_S3_UPDATE_CONFIG_XATTR      = "curvefs.s3.update.config"
 )

 type S3InfoObj struct {
	AK         string `json:"ak,omitempty"`
	SK         string `json:"sk,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	Bucketname string `json:"bucketname,omitempty"`
}
 
 type UpdateS3InfoCommand struct {
	 basecmd.FinalCurveCmd
	 CtrlFilePath           string // path in user system
	 MountpointInfo         *mountinfo.MountInfo
	 S3InfoByte             []byte
 }
 
 var _ basecmd.FinalCurveCmdFunc = (*UpdateS3InfoCommand)(nil)

 func fileNotExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return os.IsNotExist(err)
}
 
 func NewUpdateS3InfoCommand() *UpdateS3InfoCommand {
	uCmd := &UpdateS3InfoCommand{
		 FinalCurveCmd: basecmd.FinalCurveCmd{
			 Use:     "s3info",
			 Short:   "update s3 info for both MDS and Fuse client",
			 Example: updateS3InfoExample,
		 },
	 }
	 basecmd.NewFinalCurveCli(&uCmd.FinalCurveCmd, uCmd)
	 return uCmd
 }
 
 func NewUpdateCommand() *cobra.Command {
	 return NewUpdateS3InfoCommand().Cmd
 }
 
 func (uCmd *UpdateS3InfoCommand) AddFlags() {
	 config.AddS3AkOptionFlag(uCmd.Cmd)
	 config.AddS3SkOptionFlag(uCmd.Cmd)
	 config.AddS3EndpointOptionFlag(uCmd.Cmd)
	 config.AddS3BucknameOptionFlag(uCmd.Cmd)
 }
 
 func (uCmd *UpdateS3InfoCommand) Init(cmd *cobra.Command, args []string) error {
	 // check args
	 ak := config.GetS3AkOptionFlag(uCmd.Cmd)
	 sk := config.GetS3SkOptionFlag(uCmd.Cmd)
	 endpoint := config.GetS3EndpointOptionFlag(uCmd.Cmd)
	 bucketName := config.GetS3BucknameOptionFlag(uCmd.Cmd)

	 if len(args) == 0 {
		 return fmt.Errorf("no mountpath has been specified")
	 }

	 // check has curvefs mountpoint
	 mountpoints, err := cobrautil.GetCurveFSMountPoints()
	 if err.TypeCode() != cmderror.CODE_SUCCESS {
		 return err.ToError()
	 } else if len(mountpoints) == 0 {
		 return errors.New("no curvefs mountpoint has been found")
	 }
 
	 absPath, _ := filepath.Abs(args[0])
	 uCmd.CtrlFilePath = ""
	 for _, mountpointInfo := range mountpoints {
		 rel, err := filepath.Rel(mountpointInfo.MountPoint, absPath)
		 if err == nil && !strings.HasPrefix(rel, "..") {
			 // found the mountpoint
			 if uCmd.MountpointInfo == nil ||
				 len(uCmd.MountpointInfo.MountPoint) < len(mountpointInfo.MountPoint) {
				 // Prevent the curvefs directory from being mounted under the curvefs directory
				 // /a/b/c:
				 // test-1 mount in /a
				 // test-1 mount in /a/b
				 // warmup /a/b/c.
				 uCmd.CtrlFilePath = fmt.Sprintf("%s/%s", absPath, config.CURVEFS_CTRL_FILE)
			 }
		 }
	 }

	 var s3Info S3InfoObj
	 if ak != config.FLAG2DEFAULT[config.CURVEFS_S3_AK] {
		s3Info.AK = ak
   }
	 if sk != config.FLAG2DEFAULT[config.CURVEFS_S3_SK] {
		s3Info.SK = sk
   }
	 if endpoint != config.FLAG2DEFAULT[config.CURVEFS_S3_ENDPOINT] {
		s3Info.Endpoint = endpoint
   }
	 if bucketName != config.FLAG2DEFAULT[config.CURVEFS_S3_BUCKETNAME] {
		s3Info.Bucketname = bucketName
   }

	 jsonData, errStr := json.Marshal(s3Info)
	 // Serialize the S3InfoObj struct to JSON bytes
	 if errStr != nil {
		 convertErr := cmderror.ErrSetxattr()
		 convertErr.Format(CURVEFS_S3_UPDATE_CONFIG_XATTR, err)
		 return convertErr.ToError()
 	 }

	 uCmd.S3InfoByte = make([]byte, len(jsonData))
	 copy(uCmd.S3InfoByte, jsonData)

	 return nil
 }
 
 func (uCmd *UpdateS3InfoCommand) Print(cmd *cobra.Command, args []string) error {
	 return output.FinalCmdOutput(&uCmd.FinalCurveCmd, uCmd)
 }
 
 func (uCmd *UpdateS3InfoCommand) RunCommand(cmd *cobra.Command, args []string) error {
   var createFileErr error
	 var file *os.File
	 createFileErr = nil
	 if fileNotExists(uCmd.CtrlFilePath) {
		 file, createFileErr = os.Create(uCmd.CtrlFilePath)
	 }

	 if createFileErr != nil {
		 return fmt.Errorf("there was an error creating temporary control file: %s", createFileErr)
	 }

	 defer file.Close()

	 err := unix.Setxattr(uCmd.CtrlFilePath, CURVEFS_S3_UPDATE_CONFIG_XATTR, uCmd.S3InfoByte, 0)
	 if err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
		 return fmt.Errorf("filesystem does not support extended attributes")
	 } else if err != nil {
		 setErr := cmderror.ErrSetxattr()
		 setErr.Format(CURVEFS_S3_UPDATE_CONFIG_XATTR, err.Error())
		 return setErr.ToError()
	 }

	 fmt.Println("s3 info is updated.")

	 return nil
 }
 
 func (uCmd *UpdateS3InfoCommand) ResultPlainOutput() error {
	 return output.FinalCmdOutputPlain(&uCmd.FinalCurveCmd)
 }
 