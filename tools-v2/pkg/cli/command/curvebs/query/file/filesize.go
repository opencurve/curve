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
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type GetFileSizeRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.GetFileSizeRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*GetFileSizeRpc)(nil) // check interface

type GetFileSizeCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *GetFileSizeRpc
	Response *nameserver2.GetFileSizeResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetFileSizeCommand)(nil) // check interface

func (gRpc *GetFileSizeRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (gRpc *GetFileSizeRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetFileSize(ctx, gRpc.Request)
}

func NewFileSizeCommand() *cobra.Command {
	return NewGetFileSizeCommand().Cmd
}

func NewGetFileSizeCommand() *GetFileSizeCommand {
	fileCmd := &GetFileSizeCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&fileCmd.FinalCurveCmd, fileCmd)
	return fileCmd
}

func (gCmd *GetFileSizeCommand) AddFlags() {
	config.AddBsMdsFlagOption(gCmd.Cmd)
	config.AddRpcRetryTimesFlag(gCmd.Cmd)
	config.AddRpcTimeoutFlag(gCmd.Cmd)
	config.AddBsPathRequiredFlag(gCmd.Cmd)
}

func (gCmd *GetFileSizeCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsAddrSlice(gCmd.Cmd, config.CURVEBS_MDSADDR)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(gCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(gCmd.Cmd, config.RPCRETRYTIMES)
	filepath := config.GetBsFlagString(gCmd.Cmd, config.CURVEBS_PATH)
	request := nameserver2.GetFileSizeRequest{
		FileName: &filepath,
	}
	gCmd.Rpc = &GetFileSizeRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetFileSize"),
		Request: &request,
	}
	return nil
}

func (gCmd *GetFileSizeCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&gCmd.FinalCurveCmd, gCmd)
}

func (gCmd *GetFileSizeCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(gCmd.Rpc.Info, gCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	gCmd.Response = result.(*nameserver2.GetFileSizeResponse)
	if gCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		retErr := cmderror.ErrBsGetFileSizeRpc(gCmd.Response.GetStatusCode(),
			gCmd.Rpc.Request.GetFileName())
		return retErr.ToError()
	}
	return nil
}

func (gCmd *GetFileSizeCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&gCmd.FinalCurveCmd)
}

func GetFileSize(caller *cobra.Command) (*nameserver2.GetFileSizeResponse, *cmderror.CmdError) {
	getCmd := NewGetFileSizeCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH,
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
