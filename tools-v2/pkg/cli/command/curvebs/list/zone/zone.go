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
 * Created Date: 2022-09-14
 * Author: chengyi (Cyber-SiKu)
 */

package zone

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	physicalpool "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/physicalPool"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type ListPoolZoneRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPoolZoneRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListPoolZoneRpc)(nil) // check interface

type PoolZoneCommand struct {
	basecmd.FinalCurveCmd
	Rpc   []*ListPoolZoneRpc
	Zones []*topology.ZoneInfo
}

var _ basecmd.FinalCurveCmdFunc = (*PoolZoneCommand)(nil) // check interface

func (lRpc *ListPoolZoneRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListPoolZoneRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListPoolZone(ctx, lRpc.Request)
}

func NewPoolZoneCommand() *cobra.Command {
	return NewListPoolZoneCommand().Cmd
}

func NewListPoolZoneCommand() *PoolZoneCommand {
	pzCmd := &PoolZoneCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use: "zone",
		},
	}

	basecmd.NewFinalCurveCli(&pzCmd.FinalCurveCmd, pzCmd)
	return pzCmd
}

func (pCmd *PoolZoneCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
}

func (pCmd *PoolZoneCommand) Init(cmd *cobra.Command, args []string) error {
	phyPools, err := physicalpool.GetPhysicalPool(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	for _, phy := range phyPools {
		rpc := &ListPoolZoneRpc{
			Request: &topology.ListPoolZoneRequest{
				PhysicalPoolID: phy.PhysicalPoolID,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListPoolZone"),
		}
		pCmd.Rpc = append(pCmd.Rpc, rpc)
	}
	return nil
}

func (pCmd *PoolZoneCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PoolZoneCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range pCmd.Rpc {
		infos = append(infos, rpc.Info)
		funcs = append(funcs, rpc)
	}
	results, errs := basecmd.GetRpcListResponse(infos, funcs)
	if len(errs) == len(infos) {
		mergeErr := cmderror.MergeCmdErrorExceptSuccess(errs)
		return mergeErr.ToError()
	}
	var errors []*cmderror.CmdError
	for _, res := range results {
		info := res.(*topology.ListPoolZoneResponse)
		if info.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
			err := cmderror.ErrBsListPoolZoneRpc(
				statuscode.TopoStatusCode(info.GetStatusCode()),
			)
			errs = append(errs, err)
			continue
		}
		zones := info.GetZones()
		pCmd.Zones = append(pCmd.Zones, zones...)
	}
	errRet := cmderror.MergeCmdError(errors)
	pCmd.Error = &errRet
	return nil
}

func (pCmd *PoolZoneCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func ListZone(caller *cobra.Command) ([]*topology.ZoneInfo, *cmderror.CmdError) {
	listPoolZone := NewListPoolZoneCommand()
	listPoolZone.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listPoolZone.Cmd.SilenceErrors = true
	config.AlignFlagsValue(caller, listPoolZone.Cmd, []string{
		config.CURVEBS_MDSADDR,
	})
	err := listPoolZone.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsListZone()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return listPoolZone.Zones, cmderror.Success()
}
