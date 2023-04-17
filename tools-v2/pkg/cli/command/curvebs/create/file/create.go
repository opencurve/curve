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
* Created Date: 2023-04-14
* Author: chengyi01
 */

package file

import (
	"context"
	"fmt"

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

type CreateFileRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.CreateFileRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*CreateFileRpc)(nil)

// CreateCommand definition
type CreateCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *CreateFileRpc
	Response *nameserver2.CreateFileResponse
}

var _ basecmd.FinalCurveCmdFunc = (*CreateCommand)(nil)

func (gRpc *CreateFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

}

func (gRpc *CreateFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.CreateFile(ctx, gRpc.Request)
}

func (cCmd *CreateCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	//get the default timeout and retrytimes
	timeout := config.GetFlagDuration(cCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(cCmd.Cmd, config.RPCRETRYTIMES)
	filename := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_PATH)
	username := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_PASSWORD)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}

	fileTypeStr := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_TYPE)
	//fileTypeStr := "dir or file"
	fileType, errType := cobrautil.TranslateFileType(fileTypeStr)
	if errType.TypeCode() != cmderror.CODE_SUCCESS {
		return errType.ToError()
	}
	createRequest := nameserver2.CreateFileRequest{
		FileName:   &filename,
		Owner:      &username,
		Date:       &date,
		FileType:   &fileType,
	}
	if fileType == nameserver2.FileType_INODE_PAGEFILE {
		sizeStr := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_SIZE)
		size, err := humanize.ParseBytes(sizeStr)
		if err != nil {
			return fmt.Errorf("parse size[%s] failed, err: %v", sizeStr, err)
		}
		createRequest.FileLength = &size

		stripeCount := config.GetBsFlagUint64(cCmd.Cmd, config.CURVEBS_STRIPE_COUNT)
		createRequest.StripeCount = &stripeCount
		stripeUnitStr := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_STRIPE_UNIT)
		stripeUnit, errUnit := humanize.ParseBytes(stripeUnitStr)
		if errUnit != nil {
			return fmt.Errorf("parse stripe unit[%s] failed, err: %v", stripeUnitStr, errUnit)
		}
		createRequest.StripeUnit = &stripeUnit
	}
	if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, username)
		sig := cobrautil.CalcString2Signature(strSig, password)
		createRequest.Signature = &sig
	}
	cCmd.Rpc = &CreateFileRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "CreateFile"),
		Request: &createRequest,
	}
	return nil
}

func (cCmd *CreateCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	cCmd.Response = result.(*nameserver2.CreateFileResponse)
	if cCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrCreateFile(cCmd.Response.GetStatusCode(), cCmd.Rpc.Request.GetFileName())
		return err.ToError()
	}
	return nil
}

func (cCmd *CreateCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CreateCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

func (cCmd *CreateCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddBsPathRequiredFlag(cCmd.Cmd)
	config.AddBsUserOptionFlag(cCmd.Cmd)
	config.AddBsPasswordOptionFlag(cCmd.Cmd)
	config.AddBsSizeOptionFlag(cCmd.Cmd)
	config.AddBsFileTypeRequiredFlag(cCmd.Cmd)
	config.AddBsStripeUnitOptionFlag(cCmd.Cmd)
	config.AddBsStripeCountOptionFlag(cCmd.Cmd)
}

// NewCreateCommand return the mid cli
func NewCreateCommand() *CreateCommand {
	cCmd := &CreateCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd
}

func CreateFileOrDirectory(caller *cobra.Command) (*nameserver2.CreateFileResponse, *cmderror.CmdError) {
	createCmd := NewCreateCommand()
	config.AlignFlagsValue(caller, createCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
		config.CURVEBS_SIZE, config.CURVEBS_TYPE, config.CURVEBS_STRIPE_UNIT,
		config.CURVEBS_STRIPE_COUNT,
	})
	createCmd.Cmd.SilenceErrors = true
	createCmd.Cmd.SilenceUsage = true
	createCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := createCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsCreateFileOrDirectoryType()
		retErr.Format(err.Error())
		return createCmd.Response, retErr
	}
	return createCmd.Response, cmderror.Success()
}
