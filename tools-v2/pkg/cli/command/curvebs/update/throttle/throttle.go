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
* Created Date: 2023-04-24
* Author: chengyi01
 */

package throttle

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

type UpdateFileThrottleRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.UpdateFileThrottleParamsRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*UpdateFileThrottleRpc)(nil) // check interface

func (uRpc *UpdateFileThrottleRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	uRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (uRpc *UpdateFileThrottleRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return uRpc.mdsClient.UpdateFileThrottleParams(ctx, uRpc.Request)
}

type ThrottleCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *UpdateFileThrottleRpc
	Response *nameserver2.UpdateFileThrottleParamsResponse
}

var _ basecmd.FinalCurveCmdFunc = (*ThrottleCommand)(nil) // check interface

func NewUpdateThrottleCommand() *ThrottleCommand {
	throttleCmd := &ThrottleCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "throttle",
			Short:   "update file throttle params",
		},
	}
	basecmd.NewFinalCurveCli(&throttleCmd.FinalCurveCmd, throttleCmd)
	return throttleCmd
}

func NewThrottleCommand() *cobra.Command {
	return NewUpdateThrottleCommand().Cmd
}

func (tCmd *ThrottleCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(tCmd.Cmd)
	config.AddRpcTimeoutFlag(tCmd.Cmd)
	config.AddBsMdsFlagOption(tCmd.Cmd)
	config.AddBsPathRequiredFlag(tCmd.Cmd)
	config.AddBsThrottleTypeRequiredFlag(tCmd.Cmd)
	config.AddBsUserOptionFlag(tCmd.Cmd)
	config.AddBsPasswordOptionFlag(tCmd.Cmd)
	config.AddBsLimitRequiredFlag(tCmd.Cmd)
	config.AddBsBurstOptionFlag(tCmd.Cmd)
	config.AddBsBurstLengthOptionFlag(tCmd.Cmd)
}

func (tCmd *ThrottleCommand) Init(cmd *cobra.Command, args []string) error {
	path := config.GetBsFlagString(cmd, config.CURVEBS_PATH)
	throttleTypeStr := config.GetBsFlagString(cmd, config.CURVEBS_TYPE)
	throttleType, err := cobrautil.ParseThrottleType(throttleTypeStr)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	limit := config.GetBsFlagUint64(cmd, config.CURVEBS_LIMIT)
	params := &nameserver2.ThrottleParams{
		Type:  &throttleType,
		Limit: &limit,
	}
	if config.GetFlagChanged(cmd, config.CURVEBS_BURST) {
		burst := config.GetBsFlagUint64(cmd, config.CURVEBS_BURST)
		burstLength := config.GetBsFlagUint64(cmd, config.CURVEBS_BURST_LENGTH)
		if burstLength == 0 {
			burstLength = 1
		}
		params.Burst = &burst
		params.BurstLength = &burstLength
	}
	date, errDat := cobrautil.GetTimeofDayUs()
	owner := config.GetBsFlagString(tCmd.Cmd, config.CURVEBS_USER)
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
	request := &nameserver2.UpdateFileThrottleParamsRequest{
		FileName:       &path,
		Owner:          &owner,
		Date:           &date,
		ThrottleParams: params,
	}
	password := config.GetBsFlagString(tCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		request.Signature = &sig
	}
	mdsAddrs, errMds := config.GetBsMdsAddrSlice(tCmd.Cmd)
	if errMds.TypeCode() != cmderror.CODE_SUCCESS {
		return errMds.ToError()
	}
	timeout := config.GetFlagDuration(tCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(tCmd.Cmd, config.RPCRETRYTIMES)
	tCmd.Rpc = &UpdateFileThrottleRpc{
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "UpdateFileThrottle"),
		Request: request,
	}
	tCmd.SetHeader([]string{cobrautil.ROW_RESULT})
	return nil
}

func (tCmd *ThrottleCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&tCmd.FinalCurveCmd, tCmd)
}

func (tCmd *ThrottleCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(tCmd.Rpc.Info, tCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	tCmd.Response = result.(*nameserver2.UpdateFileThrottleParamsResponse)
	tCmd.Result = tCmd.Response
	tCmd.Error = cmderror.ErrUpdateFileThrottle(tCmd.Response.GetStatusCode(), tCmd.Rpc.Request.GetFileName())
	tCmd.TableNew.Append([]string{tCmd.Error.Message})
	if tCmd.Error.TypeCode() != cmderror.CODE_SUCCESS {
		return tCmd.Error.ToError()
	}
	return nil
}

func (tCmd *ThrottleCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd)
}
