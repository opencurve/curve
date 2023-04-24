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
 * Created Date: 2022-11-17
 * Author: Sindweller
 */

package dir

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	dirExample = `$ curve bs list dir --path /`
)

type ListDirRpc struct {
	Info          *basecmd.Rpc
	Request       *nameserver2.ListDirRequest
	curveFSClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ListDirRpc)(nil) // check interface

type DirCommand struct {
	basecmd.FinalCurveCmd
	Rpc []*ListDirRpc
}

var _ basecmd.FinalCurveCmdFunc = (*DirCommand)(nil) // check interface

func (lRpc *ListDirRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.curveFSClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (lRpc *ListDirRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.curveFSClient.ListDir(ctx, lRpc.Request)
}

func NewDirCommand() *cobra.Command {
	return NewListDirCommand().Cmd
}

func NewListDirCommand() *DirCommand {
	lsCmd := &DirCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "dir",
			Short:   "list directory information in curvebs",
			Example: dirExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

// AddFlags implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddBsPathOptionFlag(pCmd.Cmd)
	config.AddBsUserOptionFlag(pCmd.Cmd)
	config.AddBsPasswordOptionFlag(pCmd.Cmd)
}

// Init implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	fileName := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PATH)
	owner := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_USER)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	rpc := &ListDirRpc{
		Request: &nameserver2.ListDirRequest{
			FileName: &fileName,
			Owner:    &owner,
			Date:     &date,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListDir"),
	}
	// auth
	password := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		rpc.Request.Signature = &sig
	}
	pCmd.Rpc = append(pCmd.Rpc, rpc)
	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_FILE_NAME,
		cobrautil.ROW_PARENT_ID,
		cobrautil.ROW_FILE_TYPE,
		cobrautil.ROW_OWNER,
		cobrautil.ROW_CTIME,
		cobrautil.ROW_ALLOC_SIZE,
		cobrautil.ROW_FILE_SIZE,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{cobrautil.ROW_OWNER, cobrautil.ROW_FILE_TYPE, cobrautil.ROW_PARENT_ID},
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range pCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	var errors []*cmderror.CmdError
	rows := make([]map[string]string, 0)
	for _, res := range results {
		infos := res.(*nameserver2.ListDirResponse).GetFileInfo()
		for _, info := range infos {
			row := make(map[string]string)
			dirName := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PATH)
			var fileName string
			if dirName == "/" {
				fileName = dirName + info.GetFileName()
			} else {
				fileName = dirName + "/" + info.GetFileName()
			}
			row[cobrautil.ROW_ID] = fmt.Sprintf("%d", info.GetId())
			row[cobrautil.ROW_FILE_NAME] = fileName
			row[cobrautil.ROW_PARENT_ID] = fmt.Sprintf("%d", info.GetParentId())
			row[cobrautil.ROW_FILE_TYPE] = fmt.Sprintf("%v", info.GetFileType())
			row[cobrautil.ROW_OWNER] = info.GetOwner()
			row[cobrautil.ROW_CTIME] = time.Unix(int64(info.GetCtime()/1000000), 0).Format("2006-01-02 15:04:05")

			// generate a query file command
			fInfoCmd := file.NewQueryFileCommand()
			config.AlignFlagsValue(pCmd.Cmd, fInfoCmd.Cmd, []string{
				config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
				config.CURVEBS_PATH,
			})
			fInfoCmd.Cmd.Flags().Set("path", fileName)

			// Get file size
			sizeRes, err := file.GetFileSize(fInfoCmd.Cmd)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				// TODO handle err
				log.Printf("%s failed to get file size: %v", info.GetFileName(), err)
				//return err.ToError()
			}
			row[cobrautil.ROW_FILE_SIZE] = fmt.Sprintf("%s", humanize.IBytes(sizeRes.GetFileSize()))
			// Get allocated size
			allocRes, err := file.GetAllocatedSize(fInfoCmd.Cmd)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				// TODO handle err
				log.Printf("%s failed to get allocated size: %v", info.GetFileName(), err)
				//return err.ToError()
			}
			row[cobrautil.ROW_ALLOC_SIZE] = fmt.Sprintf("%s", humanize.IBytes(allocRes.GetAllocatedSize()))
			rows = append(rows, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{cobrautil.ROW_OWNER, cobrautil.ROW_FILE_TYPE, cobrautil.ROW_PARENT_ID})
	pCmd.TableNew.AppendBulk(list)
	errRet := cmderror.MergeCmdError(errors)
	pCmd.Error = errRet
	pCmd.Result = results
	return nil
}

// ResultPlainOutput implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}
