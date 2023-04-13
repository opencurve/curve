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
 * Project: CurveCli
 * Created Date: 2023-04-11
 * Author: chengyi (Cyber-SiKu)
 */

package seginfo

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

type GetSegmentRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.GetOrAllocateSegmentRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*GetSegmentRpc)(nil) // check interface

func (gRpc *GetSegmentRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (gRpc *GetSegmentRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gRpc.mdsClient.GetOrAllocateSegment(ctx, gRpc.Request)
}

type SegmentCommand struct {
	Response *nameserver2.GetOrAllocateSegmentResponse
	Rpc      *GetSegmentRpc
	basecmd.FinalCurveCmd
}

var _ basecmd.FinalCurveCmdFunc = (*SegmentCommand)(nil) // check interface

func NewQuerySegmentCommand() *SegmentCommand {
	segmentCmd := &SegmentCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&segmentCmd.FinalCurveCmd, segmentCmd)
	return segmentCmd
}

func NewSegmentCommand() *cobra.Command {
	return NewQuerySegmentCommand().Cmd
}

func (sCmd *SegmentCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddBsPathRequiredFlag(sCmd.Cmd)
	config.AddBsOffsetRequiredFlag(sCmd.Cmd)
	config.AddBsUserOptionFlag(sCmd.Cmd)
	config.AddBsPasswordOptionFlag(sCmd.Cmd)
}

func (sCmd *SegmentCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(sCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(sCmd.Cmd, config.RPCRETRYTIMES)
	filepath := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PATH)
	owner := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_USER)
	date, errDat := cobrautil.GetTimeofDayUs()
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	offset := config.GetBsFlagUint64(sCmd.Cmd, config.CURVEBS_OFFSET)
	allocate := false
	request := nameserver2.GetOrAllocateSegmentRequest{
		FileName:           &filepath,
		Offset:             &offset,
		Owner:              &owner,
		Date:               &date,
		AllocateIfNotExist: &allocate,
	}
	password := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		request.Signature = &sig
	}
	sCmd.Rpc = &GetSegmentRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "GetOrAllocateSegment"),
		Request: &request,
	}
	return nil
}

func (sCmd *SegmentCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SegmentCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.Response = result.(*nameserver2.GetOrAllocateSegmentResponse)
	if sCmd.Response.GetStatusCode() != nameserver2.StatusCode_kOK {
		retErr := cmderror.ErrGetOrAllocateSegment(sCmd.Response.GetStatusCode(),
			sCmd.Rpc.Request.GetFileName(), sCmd.Rpc.Request.GetOffset())
		return retErr.ToError()
	}
	return nil
}

func (sCmd *SegmentCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func GetSegment(caller *cobra.Command) (*nameserver2.GetOrAllocateSegmentResponse, *cmderror.CmdError) {
	getCmd := NewQuerySegmentCommand()
	config.AlignFlagsValue(caller, getCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
		config.CURVEBS_OFFSET,
	})
	getCmd.Cmd.SilenceErrors = true
	getCmd.Cmd.SilenceUsage = true
	getCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := getCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetSegment()
		retErr.Format(err.Error())
		return getCmd.Response, retErr
	}
	return getCmd.Response, cmderror.Success()
}
