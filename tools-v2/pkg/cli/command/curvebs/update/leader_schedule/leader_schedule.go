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
* Project: curvecli
* Created Date: 2023-06-14
* Author: montaguelhz
 */

package leader_schedule

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/schedule"
	"github.com/opencurve/curve/tools-v2/proto/proto/schedule/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type UpdateLeaderScheduleRpc struct {
	Info    *basecmd.Rpc
	Request *schedule.RapidLeaderScheduleRequst
	Client  schedule.ScheduleServiceClient
}

var _ basecmd.RpcFunc = (*UpdateLeaderScheduleRpc)(nil) // check interface

func (uRpc *UpdateLeaderScheduleRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	uRpc.Client = schedule.NewScheduleServiceClient(cc)
}

func (uRpc *UpdateLeaderScheduleRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return uRpc.Client.RapidLeaderSchedule(ctx, uRpc.Request)
}

type LeaderScheduleCommand struct {
	basecmd.FinalCurveCmd

	Rpc *UpdateLeaderScheduleRpc
}

var _ basecmd.FinalCurveCmdFunc = (*LeaderScheduleCommand)(nil) // check interface

const (
	leaderScheduleExample = `$ curve bs update leader-schedule --logicalpoolid 1
$ curve bs update leader-schedule --all`
)

func NewLeaderScheduleCommand() *cobra.Command {
	return NewUpdateLeaderScheduleCommand().Cmd
}

func (lCmd *LeaderScheduleCommand) AddFlags() {
	config.AddBsMdsFlagOption(lCmd.Cmd)
	config.AddRpcRetryTimesFlag(lCmd.Cmd)
	config.AddRpcTimeoutFlag(lCmd.Cmd)

	config.AddBsLogicalPoolIdOptionFlag(lCmd.Cmd)
	config.AddBsAllOptionFlag(lCmd.Cmd)
}

func NewUpdateLeaderScheduleCommand() *LeaderScheduleCommand {
	sCmd := &LeaderScheduleCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "leader-schedule",
			Short:   "rapidly transfer leader",
			Example: leaderScheduleExample,
		},
	}

	basecmd.NewFinalCurveCli(&sCmd.FinalCurveCmd, sCmd)
	return sCmd
}

func (lCmd *LeaderScheduleCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(lCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	header := []string{cobrautil.ROW_RESULT, cobrautil.ROW_REASON}
	lCmd.SetHeader(header)

	timeout := config.GetFlagDuration(lCmd.Cmd, config.RPCTIMEOUT)
	retryTimes := config.GetFlagInt32(lCmd.Cmd, config.RPCRETRYTIMES)

	// if flag all not changed, it must be false
	all := config.GetBsFlagBool(lCmd.Cmd, config.CURVEBS_ALL)
	islogicalPoolIDChanged := config.GetBsFlagChanged(lCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
	if !all && !islogicalPoolIDChanged {
		return fmt.Errorf("all or logicalpoolid is required")
	}

	var logicalPoolID uint32
	if all {
		logicalPoolID = 0
	} else {
		logicalPoolID = config.GetBsFlagUint32(lCmd.Cmd, config.CURVEBS_LOGIC_POOL_ID)
		if logicalPoolID == 0 {
			return fmt.Errorf("please set flag all true instead of setting flag logicalpoolid 0")
		}
	}

	lCmd.Rpc = &UpdateLeaderScheduleRpc{
		Request: &schedule.RapidLeaderScheduleRequst{
			LogicalPoolID: &logicalPoolID,
		},
		Info: basecmd.NewRpc(mdsAddrs, timeout, retryTimes, "RapidLeaderSchedule"),
	}

	return nil
}

func (lCmd *LeaderScheduleCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&lCmd.FinalCurveCmd, lCmd)
}

func (lCmd *LeaderScheduleCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(lCmd.Rpc.Info, lCmd.Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	out := make(map[string]string)
	response := result.(*schedule.RapidLeaderScheduleResponse)
	err = cmderror.ErrBsRapidLeaderSchedule(statuscode.ScheduleStatusCode(response.GetStatusCode()))
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_FAILED
	} else {
		out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	}
	out[cobrautil.ROW_REASON] = err.Message

	res := cobrautil.Map2List(out, lCmd.Header)
	lCmd.TableNew.Append(res)

	lCmd.Result = out
	lCmd.Error = err
	return nil
}

func (lCmd *LeaderScheduleCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&lCmd.FinalCurveCmd)
}
