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
* Created Date: 2023-04-13
* Author: chengyi01
 */

package file

import (
	"context"

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
	fileExample = `$ curve bs update file`
)

type ExtendFileRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.ExtendFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ExtendFileRpc)(nil) // check interface

func (eRpc *ExtendFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *ExtendFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return eRpc.mdsClient.ExtendFile(ctx, eRpc.Request)
}

type FileCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ExtendFileRpc
	Response *nameserver2.ExtendFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*FileCommand)(nil) // check interface

func NewUpdateFileCommand() *FileCommand {
	fileCmd := &FileCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "file",
			Short:   "extend volume of file to size",
			Example: fileExample,
		},
	}
	basecmd.NewFinalCurveCli(&fileCmd.FinalCurveCmd, fileCmd)
	return fileCmd
}

func NewFileCommand() *cobra.Command {
	return NewUpdateFileCommand().Cmd
}

func (uCmd *FileCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(uCmd.Cmd)
	config.AddRpcTimeoutFlag(uCmd.Cmd)
	config.AddBsMdsFlagOption(uCmd.Cmd)
	config.AddBsPathRequiredFlag(uCmd.Cmd)
	config.AddBsSizeRequiredFlag(uCmd.Cmd)
	config.AddBsUserOptionFlag(uCmd.Cmd)
	config.AddBsPasswordOptionFlag(uCmd.Cmd)
}

func (uCmd *FileCommand) Init(cmd *cobra.Command, args []string) error {
	fileName := config.GetBsFlagString(uCmd.Cmd, config.CURVEBS_PATH)
	newSize := config.GetBsFlagUint64(uCmd.Cmd, config.CURVEBS_SIZE)
	newSize = newSize * humanize.GiByte
	date, errDat := cobrautil.GetTimeofDayUs()
	owner := config.GetBsFlagString(uCmd.Cmd, config.CURVEBS_USER)
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	request := &nameserver2.ExtendFileRequest{
		FileName: &fileName,
		NewSize:  &newSize,
		Date:     &date,
		Owner:    &owner,
	}
	password := config.GetBsFlagString(uCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		request.Signature = &sig
	}
	mdsAddrs, errMds := config.GetBsMdsAddrSlice(uCmd.Cmd)
	if errMds.TypeCode() != cmderror.CODE_SUCCESS {
		return errMds.ToError()
	}
	timeout := config.GetFlagDuration(uCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(uCmd.Cmd, config.RPCRETRYTIMES)
	uCmd.Rpc = &ExtendFileRpc{
		Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ExtendFile"),
		Request: request,
	}
	uCmd.SetHeader([]string{cobrautil.ROW_RESULT})
	return nil
}

func (uCmd *FileCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&uCmd.FinalCurveCmd, uCmd)
}

func (uCmd *FileCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(uCmd.Rpc.Info, uCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	uCmd.Response = result.(*nameserver2.ExtendFileResponse)
	uCmd.Result = uCmd.Response
	sizeStr := humanize.IBytes(uCmd.Rpc.Request.GetNewSize())
	uCmd.Error = cmderror.ErrExtendFile(uCmd.Response.GetStatusCode(), *uCmd.Rpc.Request.FileName, sizeStr)
	uCmd.TableNew.Append([]string{uCmd.Error.Message})
	if uCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
		return uCmd.Error.ToError()
	}
	return nil
}

func (uCmd *FileCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&uCmd.FinalCurveCmd)
}
