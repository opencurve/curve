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
	"time"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
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
	Rpc      *ListDirRpc
	Response *nameserver2.ListDirResponse
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
	path := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PATH)
	owner := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_USER)
	date, errDat := curveutil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	pCmd.Rpc = &ListDirRpc{
		Request: &nameserver2.ListDirRequest{
			FileName: &path,
			Owner:    &owner,
			Date:     &date,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListDir"),
	}
	// auth
	password := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := curveutil.GetString2Signature(date, owner)
		sig := curveutil.CalcString2Signature(strSig, password)
		pCmd.Rpc.Request.Signature = &sig
	}

	header := []string{
		curveutil.ROW_ID,
		curveutil.ROW_FILE_NAME,
		curveutil.ROW_PARENT_ID,
		curveutil.ROW_FILE_TYPE,
		curveutil.ROW_OWNER,
		curveutil.ROW_CTIME,
		curveutil.ROW_ALLOC_SIZE,
		curveutil.ROW_FILE_SIZE,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(curveutil.GetIndexSlice(
		pCmd.Header, []string{curveutil.ROW_OWNER, curveutil.ROW_FILE_TYPE, curveutil.ROW_PARENT_ID},
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		pCmd.Error = err
		pCmd.Result = result
		return err.ToError()
	}
	pCmd.Response = result.(*nameserver2.ListDirResponse)
	infos := pCmd.Response.GetFileInfo()
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for _, info := range infos {
		row := make(map[string]string)
		var fileName string
		path := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PATH)
		if path[len(path)-1] == '/' {
			fileName = path + info.GetFileName()
		} else {
			fileName = path + "/" + info.GetFileName()
		}
		row[curveutil.ROW_ID] = fmt.Sprintf("%d", info.GetId())
		row[curveutil.ROW_FILE_NAME] = fileName
		row[curveutil.ROW_PARENT_ID] = fmt.Sprintf("%d", info.GetParentId())
		row[curveutil.ROW_FILE_TYPE] = fmt.Sprintf("%v", info.GetFileType())
		row[curveutil.ROW_OWNER] = info.GetOwner()
		row[curveutil.ROW_CTIME] = time.Unix(int64(info.GetCtime()/1000000), 0).Format("2006-01-02 15:04:05")

		// generate a query file command
		fInfoCmd := file.NewQueryFileCommand()
		config.AlignFlagsValue(pCmd.Cmd, fInfoCmd.Cmd, []string{
			config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		})
		fInfoCmd.Cmd.Flags().Set("path", fileName)

		// Get file size
		sizeRes, err := file.GetFileSize(fInfoCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
			continue
		}
		row[curveutil.ROW_FILE_SIZE] = fmt.Sprintf("%s", humanize.IBytes(sizeRes.GetFileSize()))
		// Get allocated size
		allocRes, err := file.GetAllocatedSize(fInfoCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
			continue
		}
		row[curveutil.ROW_ALLOC_SIZE] = fmt.Sprintf("%s", humanize.IBytes(allocRes.GetAllocatedSize()))
		rows = append(rows, row)
	}
	list := curveutil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{curveutil.ROW_OWNER, curveutil.ROW_FILE_TYPE, curveutil.ROW_PARENT_ID})
	pCmd.TableNew.AppendBulk(list)
	if len(errs) != 0 {
		mergeErr := cmderror.MergeCmdError(errs)
		pCmd.Result, pCmd.Error = result, mergeErr
		return mergeErr.ToError()
	}
	pCmd.Result, pCmd.Error = result, cmderror.Success()
	return nil
}

// ResultPlainOutput implements basecmd.FinalCurveCmdFunc
func (pCmd *DirCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func ListDir(caller *cobra.Command) (*nameserver2.ListDirResponse, *cmderror.CmdError) {
	lsCmd := NewListDirCommand()
	config.AlignFlagsValue(caller, lsCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
	})
	lsCmd.Cmd.SilenceErrors = true
	lsCmd.Cmd.SilenceUsage = true
	lsCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := lsCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListDir()
		retErr.Format(err.Error())
		return lsCmd.Response, retErr
	}
	return lsCmd.Response, cmderror.Success()
}
