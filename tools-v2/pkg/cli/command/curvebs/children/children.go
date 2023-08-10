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
* Project: curve
* Created Date: 2023-0815
* Author: xuchaojie
 */
package children

import (
	"context"

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
	dirExample = `$ curve bs children --path /test1 --user curve
$ curve bs children --snappath /test1/test1-1 --user curve
$ curve bs children --path /test1 --seq 1 --user curve`
)

type childrenRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.ChildrenRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*childrenRpc)(nil) // check interface

func (eRpc *childrenRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *childrenRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return eRpc.mdsClient.Children(ctx, eRpc.Request)
}

type childrenCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *childrenRpc
	Response *nameserver2.ChildrenResponse
}

var _ basecmd.FinalCurveCmdFunc = (*childrenCommand)(nil)

func (fCmd *childrenCommand) Init(cmd *cobra.Command, args []string) error {
	date, errDat := cobrautil.GetTimeofDayUs()
	owner := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
    var request *nameserver2.ChildrenRequest
    if (config.GetFlagChanged(fCmd.Cmd, config.CURVEBS_SNAPPATH)) {
        snapFileName := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_SNAPPATH)
        request = &nameserver2.ChildrenRequest {
            SnapFileName: &snapFileName,
            Owner:        &owner,
            Date:     &date,
        }
    } else {
        fileName := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PATH)
        if (config.GetFlagChanged(fCmd.Cmd, config.CURVEBS_SEQ)) {
            seq := config.GetBsFlagUint64(fCmd.Cmd, config.CURVEBS_SEQ)
            request = &nameserver2.ChildrenRequest {
                FileName: &fileName,
                Seq:      &seq,
                Owner:    &owner,
                Date:     &date,
            }
        } else {
            request = &nameserver2.ChildrenRequest {
                FileName: &fileName,
                Owner:    &owner,
                Date:     &date,
            }
        }
    }
    password := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PASSWORD)
    if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
        strSig := cobrautil.GetString2Signature(date, owner)
        sig := cobrautil.CalcString2Signature(strSig, password)
        request.Signature = &sig
    }

	mdsAddrs, errMds := config.GetBsMdsAddrSlice(fCmd.Cmd)
	if errMds.TypeCode() != cmderror.CODE_SUCCESS {
		return errMds.ToError()
	}
	timeout := config.GetFlagDuration(fCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(fCmd.Cmd, config.RPCRETRYTIMES)
    fCmd.Rpc = &childrenRpc{
        Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "Children"),
            Request: request,
    }

	header := []string{
		cobrautil.ROW_FILE_NAME,
	}
	fCmd.SetHeader(header)
	return nil
}

func (fCmd *childrenCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	fCmd.Response = result.(*nameserver2.ChildrenResponse)
    fileNames := fCmd.Response.GetFileNames()
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for _, fileName := range fileNames {
		row := make(map[string]string)
		row[cobrautil.ROW_FILE_NAME] = fileName
		rows = append(rows, row)
    }
    list := cobrautil.ListMap2ListSortByKeys(rows, fCmd.Header, []string {
        cobrautil.ROW_FILE_NAME,
    })
    fCmd.TableNew.AppendBulk(list)
	if len(errs) != 0 {
		mergeErr := cmderror.MergeCmdError(errs)
		fCmd.Result, fCmd.Error = result, mergeErr
		return mergeErr.ToError()
	}
	fCmd.Result, fCmd.Error = result, cmderror.Success()
	return nil
}

func (fCmd *childrenCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *childrenCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func (fCmd *childrenCommand) AddFlags() {
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddBsPathOptionFlag(fCmd.Cmd)
	config.AddBsSeqOptionFlag(fCmd.Cmd)
    config.AddBsSrcSnapshotPathOptionFlag(fCmd.Cmd)
	config.AddBsUserRequiredFlag(fCmd.Cmd)
	config.AddBsPasswordOptionFlag(fCmd.Cmd)
}

func NewChildrenCommand() *cobra.Command {
	fCmd := &childrenCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "children",
			Short:   "list children of snapshot/volume in curvebs cluster",
			Example: dirExample,
		},
	}
	return basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
}
