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
 * Created Date: 2022-08-29
 * Author: chengyi (Cyber-SiKu)
 */

package file

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

type GetFileInfoRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.GetFileInfoRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*GetFileInfoRpc)(nil) // check interface

type GetFileInfoCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetFileInfoRpc
	Response *nameserver2.GetFileInfoResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetFileInfoCommand)(nil) // check interface

func (gRpc *GetFileInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (gRpc *GetFileInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetFileInfo(ctx, gRpc.Request)
}

func NewFileInfoCommand() *cobra.Command {
	return NewGetFileInfoCommand().Cmd
}

func NewGetFileInfoCommand() *GetFileInfoCommand {
	fileCmd := &GetFileInfoCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&fileCmd.FinalCurveCmd, fileCmd)
	return fileCmd
}

func (gCmd *GetFileInfoCommand) AddFlags() {
	config.AddBsMdsFlagOption(gCmd.Cmd)
	config.AddRpcRetryTimesFlag(gCmd.Cmd)
	config.AddRpcTimeoutFlag(gCmd.Cmd)
	config.AddBsPathRequiredFlag(gCmd.Cmd)
	config.AddBsUserOptionFlag(gCmd.Cmd)
	config.AddBsPasswordOptionFlag(gCmd.Cmd)
}

func (gCmd *GetFileInfoCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(gCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(gCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(gCmd.Cmd, config.RPCRETRYTIMES)
	filepath := config.GetBsFlagString(gCmd.Cmd, config.CURVEBS_PATH)
	owner := config.GetBsFlagString(gCmd.Cmd, config.CURVEBS_USER)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	request := nameserver2.GetFileInfoRequest{
		FileName: &filepath,
		Owner:    &owner,
		Date:     &date,
	}
	password := config.GetBsFlagString(gCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) !=0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		request.Signature = &sig
	}
	gCmd.Rpc = &GetFileInfoRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetFileInfo"),
		Request: &request,
	}
	return nil
}

func (gCmd *GetFileInfoCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&gCmd.FinalCurveCmd, gCmd)
}

func (gCmd *GetFileInfoCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(gCmd.Rpc.Info, gCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	gCmd.Response = result.(*nameserver2.GetFileInfoResponse)
	if gCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		retErr := cmderror.ErrBsGetFileInfoRpc(gCmd.Response.GetStatusCode(),
			gCmd.Rpc.Request.GetFileName())
		return retErr.ToError()
	}
	return nil
}

func (gCmd *GetFileInfoCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&gCmd.FinalCurveCmd)
}

func GetFileInfo(caller *cobra.Command) (*nameserver2.GetFileInfoResponse, *cmderror.CmdError) {
	getCmd := NewGetFileInfoCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetFileInfo()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.Response, cmderror.Success()
}
