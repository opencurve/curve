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
 * Created Date: 2022-08-25
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

type GetAllocatedSizeRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.GetAllocatedSizeRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*GetAllocatedSizeRpc)(nil) // check interface

type GetAllocatedSizeCommand struct {
	basecmd.FinalCurveCmd
	Rpc *GetAllocatedSizeRpc
	Response *nameserver2.GetAllocatedSizeResponse
}

var _ basecmd.FinalCurveCmdFunc = (*GetAllocatedSizeCommand)(nil) // check interface

func (gRpc *GetAllocatedSizeRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (gRpc *GetAllocatedSizeRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetAllocatedSize(ctx, gRpc.Request)
}

func NewAllocatedCommand() *cobra.Command {
	return NewGetAllocatedSizeCommand().Cmd
}

func NewGetAllocatedSizeCommand() *GetAllocatedSizeCommand {
	fileCmd := &GetAllocatedSizeCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&fileCmd.FinalCurveCmd, fileCmd)
	return fileCmd
}

func (gCmd *GetAllocatedSizeCommand) AddFlags() {
	config.AddBsMdsFlagOption(gCmd.Cmd)
	config.AddRpcRetryTimesFlag(gCmd.Cmd)
	config.AddRpcTimeoutFlag(gCmd.Cmd)
	config.AddBsPathRequiredFlag(gCmd.Cmd)
}

func (gCmd *GetAllocatedSizeCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsAddrSlice(gCmd.Cmd, config.CURVEBS_MDSADDR)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(gCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(gCmd.Cmd, config.RPCRETRYTIMES)
	filepath := config.GetBsFlagString(gCmd.Cmd, config.CURVEBS_PATH)
	request := nameserver2.GetAllocatedSizeRequest{FileName: &filepath}
	gCmd.Rpc = &GetAllocatedSizeRpc{
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetAllocatedSize"),
		Request: &request,
	}
	return nil
}

func (gCmd *GetAllocatedSizeCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&gCmd.FinalCurveCmd, gCmd)
}

func (gCmd *GetAllocatedSizeCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(gCmd.Rpc.Info, gCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	gCmd.Response = result.(*nameserver2.GetAllocatedSizeResponse)
	if gCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsGetAllocatedSizeRpc(gCmd.Response.GetStatusCode(),
			gCmd.Rpc.Request.GetFileName())
		return err.ToError()
	}
	return nil
}

func (gCmd *GetAllocatedSizeCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&gCmd.FinalCurveCmd)
}


func GetAllocatedSize(caller *cobra.Command) (*nameserver2.GetAllocatedSizeResponse, *cmderror.CmdError) {
	getCmd := NewGetAllocatedSizeCommand()
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	getCmd.Cmd.SilenceErrors = true
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH,
	})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetAllocatedSize()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return getCmd.Response, cmderror.Success()
}