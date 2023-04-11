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

package server

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/zone"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	serverExample = `$ curve bs list server`
)

type ListServerRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListZoneServerRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListServerRpc)(nil) // check interface

type ServerCommand struct {
	basecmd.FinalCurveCmd
	Rpc []*ListServerRpc
}

var _ basecmd.FinalCurveCmdFunc = (*ServerCommand)(nil) // check interface

func (lRpc *ListServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListZoneServer(ctx, lRpc.Request)
}

func NewServerCommand() *cobra.Command {
	return NewListServerCommand().Cmd
}

func NewListServerCommand() *ServerCommand {
	lsCmd := &ServerCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "server",
			Short:   "list all server information in curvebs",
			Example: serverExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

func (pCmd *ServerCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
}

func (pCmd *ServerCommand) Init(cmd *cobra.Command, args []string) error {
	zones, err := zone.ListZone(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)
	for _, zone := range zones {
		id := zone.GetZoneID()
		rpc := &ListServerRpc{
			Request: &topology.ListZoneServerRequest{
				ZoneID: &id,
			},
			Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListPoolZone"),
		}
		pCmd.Rpc = append(pCmd.Rpc, rpc)
	}
	header := []string{cobrautil.ROW_ID, cobrautil.ROW_HOSTNAME, 
		cobrautil.ROW_ZONE, cobrautil.ROW_PHYPOOL, cobrautil.ROW_INTERNAL_ADDR,
		cobrautil.ROW_EXTERNAL_ADDR,
	}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, []string{cobrautil.ROW_PHYPOOL, cobrautil.ROW_ZONE},
	))
	return nil
}

func (pCmd *ServerCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *ServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	rows := make([]map[string]string, 0)
	for _, res := range results {
		infos := res.(*topology.ListZoneServerResponse).GetServerInfo()
		for _, info := range infos {
			row := make(map[string]string)
			row[cobrautil.ROW_ID] = fmt.Sprintf("%d", info.GetServerID())
			row[cobrautil.ROW_HOSTNAME] = info.GetHostName()
			row[cobrautil.ROW_ZONE] = fmt.Sprintf("%d", info.GetZoneID())
			row[cobrautil.ROW_PHYPOOL] = fmt.Sprintf("%d", info.GetPhysicalPoolID())
			row[cobrautil.ROW_INTERNAL_ADDR] = fmt.Sprintf("%s:%d", 
				info.GetInternalIp(), info.GetInternalPort())
			row[cobrautil.ROW_EXTERNAL_ADDR] = fmt.Sprintf("%s:%d",
				info.GetExternalIp(), info.GetExternalPort())
			rows = append(rows, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string {
		cobrautil.ROW_PHYPOOL, cobrautil.ROW_ZONE,
	})
	pCmd.TableNew.AppendBulk(list)
	errRet := cmderror.MergeCmdError(errors)
	pCmd.Error = errRet
	pCmd.Result = results
	return nil
}

func (pCmd *ServerCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}
