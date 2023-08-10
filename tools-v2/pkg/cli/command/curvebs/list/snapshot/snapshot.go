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
 * Created Date: 2023-06-20
 * Author: xuchaojie
 */

package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	snapShotExample = `$ curve bs list snapshot --path / --user curve`
)

type ListSnapShotRpc struct {
	Info          *basecmd.Rpc
	Request       *nameserver2.ListSnapShotFileInfoRequest
	curveFSClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ListSnapShotRpc)(nil) // check interface

type SnapShotCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ListSnapShotRpc
	Response *nameserver2.ListSnapShotFileInfoResponse
}

var _ basecmd.FinalCurveCmdFunc = (*SnapShotCommand)(nil) // check interface

func (lRpc *ListSnapShotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.curveFSClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (lRpc *ListSnapShotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.curveFSClient.ListSnapShot(ctx, lRpc.Request)
}

func NewSnapShotCommand() *cobra.Command {
	return NewListSnapShotCommand().Cmd
};

func NewListSnapShotCommand() *SnapShotCommand {
	lsCmd := &SnapShotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "list snapshot information for volume in curvebs",
			Example: snapShotExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

// AddFlags implements basecmd.FinalCurveCmdFunc
func (pCmd *SnapShotCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddBsPathRequiredFlag(pCmd.Cmd)
	config.AddBsUserRequiredFlag(pCmd.Cmd)
	config.AddBsPasswordOptionFlag(pCmd.Cmd)
}

// Init implements basecmd.FinalCurveCmdFunc
func (pCmd *SnapShotCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	path := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PATH)
	owner := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_USER)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	pCmd.Rpc = &ListSnapShotRpc{
		Request: &nameserver2.ListSnapShotFileInfoRequest{
			FileName: &path,
			Owner:    &owner,
			Date:     &date,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListSnapShot"),
	}
	// auth
	password := config.GetBsFlagString(pCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		pCmd.Rpc.Request.Signature = &sig
	}

	header := []string{
		cobrautil.ROW_ID,
		cobrautil.ROW_FILE_NAME,
		cobrautil.ROW_PARENT_ID,
		cobrautil.ROW_FILE_TYPE,
		cobrautil.ROW_OWNER,
		cobrautil.ROW_CTIME,
		cobrautil.ROW_FILE_SIZE,
        cobrautil.ROW_SEQ,
        cobrautil.ROW_ISPROTECT,
        cobrautil.ROW_CHILDREN_COUNT,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{cobrautil.ROW_OWNER, cobrautil.ROW_FILE_TYPE, cobrautil.ROW_PARENT_ID},
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *SnapShotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *SnapShotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(pCmd.Rpc.Info, pCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		pCmd.Error = err
		pCmd.Result = result
		return err.ToError()
	}
	pCmd.Response = result.(*nameserver2.ListSnapShotFileInfoResponse)
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
		row[cobrautil.ROW_ID] = fmt.Sprintf("%d", info.GetId())
		row[cobrautil.ROW_FILE_NAME] = fileName
		row[cobrautil.ROW_PARENT_ID] = fmt.Sprintf("%d", info.GetParentId())
		row[cobrautil.ROW_FILE_TYPE] = fmt.Sprintf("%v", info.GetFileType())
		row[cobrautil.ROW_OWNER] = info.GetOwner()
		row[cobrautil.ROW_CTIME] = time.Unix(int64(info.GetCtime()/1000000), 0).Format("2006-01-02 15:04:05")
        row[cobrautil.ROW_FILE_SIZE] = fmt.Sprintf("%s", humanize.IBytes(info.GetLength()))
        row[cobrautil.ROW_SEQ] = fmt.Sprintf("%d", info.GetSeqNum())
        row[cobrautil.ROW_ISPROTECT] = fmt.Sprintf("%t", info.GetIsProtected())
        row[cobrautil.ROW_CHILDREN_COUNT] = fmt.Sprintf("%d", len(info.GetChildren()))
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{cobrautil.ROW_OWNER, cobrautil.ROW_FILE_TYPE, cobrautil.ROW_PARENT_ID})
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
func (pCmd *SnapShotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func ListSnapShot(caller *cobra.Command) (*nameserver2.ListSnapShotFileInfoResponse, *cmderror.CmdError) {
	lsCmd := NewListSnapShotCommand()
	config.AlignFlagsValue(caller, lsCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
	})
	lsCmd.Cmd.SilenceErrors = true
	lsCmd.Cmd.SilenceUsage = true
	lsCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := lsCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListSnapShot()
		retErr.Format(err.Error())
		return lsCmd.Response, retErr
	}
	return lsCmd.Response, cmderror.Success()
}
