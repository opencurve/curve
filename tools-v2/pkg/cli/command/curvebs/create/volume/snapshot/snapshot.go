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
 * Project: tools-v2
 * Created Date: 2023-7-24
 * Author: ApiaoSamaa
 */
package snapshot

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
)

const (
	createSnapShotExample = `curve bs create volume snapshot --path /curvebs-file-path --user [username] [--password password]`
)

type CreateSnapShotRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.CreateSnapShotRequest
	mdsClient nameserver2.CurveFSServiceClient
}

type CreateCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *CreateSnapShotRpc
	Response *nameserver2.CreateSnapShotResponse
}

var _ basecmd.FinalCurveCmdFunc = (*CreateCommand)(nil)

func (gRpc *CreateSnapShotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

}

func (gRpc *CreateSnapShotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.CreateSnapShot(ctx, gRpc.Request)
}

func (CreateCommand *CreateCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(CreateCommand.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(CreateCommand.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(CreateCommand.Cmd, config.RPCRETRYTIMES)
	path := config.GetBsFlagString(CreateCommand.Cmd, config.CURVEBS_PATH)
	username := config.GetBsFlagString(CreateCommand.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(CreateCommand.Cmd, config.CURVEBS_PASSWORD)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	createRequest := nameserver2.CreateSnapShotRequest{
		FileName: &path,
		Owner:    &username,
		Date:     &date,
	}

	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		createRequest.Signature = &sig
	}
	CreateCommand.Rpc = &CreateSnapShotRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "CreateSnapShot"),
		Request: &createRequest,
	}
	header := []string{cobrautil.ROW_RESULT}
	CreateCommand.SetHeader(header)
	CreateCommand.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		CreateCommand.Header, header,
	))
	return nil
}

func (CreateCommand *CreateCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(CreateCommand.Rpc.Info, CreateCommand.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		CreateCommand.Error = err
		CreateCommand.Result = result
		return err.ToError()
	}
	CreateCommand.Response = result.(*nameserver2.CreateSnapShotResponse)
	if CreateCommand.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrCreateSnapShotFile(CreateCommand.Response.GetStatusCode(), CreateCommand.Rpc.Request.GetFileName())
		return err.ToError()
	}
	out := make(map[string]string)
	out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT})
	CreateCommand.TableNew.Append(list)

	CreateCommand.Result, CreateCommand.Error = result, cmderror.Success()
	return nil
}

func (CreateCommand *CreateCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&CreateCommand.FinalCurveCmd, CreateCommand)
}

func (CreateCommand *CreateCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&CreateCommand.FinalCurveCmd)
}

func (CreateCommand *CreateCommand) AddFlags() {
	config.AddBsMdsFlagOption(CreateCommand.Cmd)
	config.AddRpcTimeoutFlag(CreateCommand.Cmd)
	config.AddRpcRetryTimesFlag(CreateCommand.Cmd)
	config.AddBsPathRequiredFlag(CreateCommand.Cmd)
	config.AddBsUserOptionFlag(CreateCommand.Cmd)
	config.AddBsPasswordOptionFlag(CreateCommand.Cmd)
}

func NewCreateSnapShotCommand() *CreateCommand {
	CreateCommand := &CreateCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "create volumn snapshot in curvebs",
			Example: createSnapShotExample,
		},
	}
	basecmd.NewFinalCurveCli(&CreateCommand.FinalCurveCmd, CreateCommand)
	return CreateCommand
}

func NewSnapShotCommand() *cobra.Command {
	return NewCreateSnapShotCommand().Cmd
}

func CreateFile(caller *cobra.Command) (*nameserver2.CreateSnapShotResponse, *cmderror.CmdError) {
	creCmd := NewCreateSnapShotCommand()
	config.AlignFlagsValue(caller, creCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
	})
	creCmd.Cmd.SilenceErrors = true
	creCmd.Cmd.SilenceUsage = true
	creCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := creCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsCreateFileOrDirectoryType()
		retErr.Format(err.Error())
		return creCmd.Response, retErr
	}
	return creCmd.Response, cmderror.Success()
}
