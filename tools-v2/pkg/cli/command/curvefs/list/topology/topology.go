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
 * Created Date: 2022-05-30
 * Author: chengyi (Cyber-SiKu)
 */

package topology

import (
	"context"
	"fmt"
	"sort"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	topology "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

const (
	topologyExample = `$ curve fs list topology`
)

type ListTopologyRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListTopologyRequest
	topologyClient topology.TopologyServiceClient
	Response       *topology.ListTopologyResponse
}

var _ basecmd.RpcFunc = (*ListTopologyRpc)(nil) // check interface

type TopologyCommand struct {
	basecmd.FinalCurveCmd
	Rpc          ListTopologyRpc
	externalAddr []string
	internalAddr []string
}

var _ basecmd.FinalCurveCmdFunc = (*TopologyCommand)(nil) // check interface

func (lRpc *ListTopologyRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListTopologyRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListTopology(ctx, lRpc.Request)
}

func NewTopologyCommand() *cobra.Command {
	return NewListTopologyCommand().Cmd
}

func GetMetaserverAddrs(caller *cobra.Command) ([]string, []string, *cmderror.CmdError) {
	listTopo := NewListTopologyCommand()
	listTopo.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listTopo.Cmd.SilenceErrors = true
	config.AlignFlagsValue(caller, listTopo.Cmd, []string{})
	err := listTopo.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetMetaserverAddr()
		retErr.Format(err.Error())
		return nil, nil, retErr
	}
	return listTopo.externalAddr, listTopo.internalAddr, cmderror.ErrSuccess()
}

func GetTopology(caller *cobra.Command) (*topology.ListTopologyResponse, *cmderror.CmdError) {
	listTopo := NewListTopologyCommand()
	listTopo.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listTopo.Cmd.SilenceErrors = true
	config.AlignFlagsValue(caller, listTopo.Cmd, []string{})
	err := listTopo.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetMetaserverAddr()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return listTopo.Rpc.Response, cmderror.ErrSuccess()
}

func NewListTopologyCommand() *TopologyCommand {
	topologyCmd := &TopologyCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "topology",
			Short:   "list the topology of the curvefs",
			Example: topologyExample,
		},
	}
	basecmd.NewFinalCurveCli(&topologyCmd.FinalCurveCmd, topologyCmd)
	return topologyCmd
}

func (tCmd *TopologyCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(tCmd.Cmd)
	config.AddRpcTimeoutFlag(tCmd.Cmd)
	config.AddFsMdsAddrFlag(tCmd.Cmd)
}

func (tCmd *TopologyCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(tCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	tCmd.Rpc.Request = &topology.ListTopologyRequest{}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	tCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListTopology")

	// header := []string{curveutil.ROW_ID, curveutil.ROW_TYPE, curveutil.ROW_NAME, curveutil.ROW_CHILD_TYPE, curveutil.ROW_CHILD_LIST}
	header := []string{curveutil.ROW_POOL, curveutil.ROW_ZONE, curveutil.ROW_SERVER, curveutil.ROW_METASERVER}
	tCmd.SetHeader(header)
	var mergeIndex []int
	mergeRow := []string{curveutil.ROW_POOL, curveutil.ROW_ZONE, curveutil.ROW_SERVER}
	for _, row := range mergeRow {
		index := slices.Index(header, row)
		mergeIndex = append(mergeIndex, index)
	}
	tCmd.TableNew.SetAutoMergeCellsByColumnIndex(mergeIndex)

	return nil
}

func (tCmd *TopologyCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&tCmd.FinalCurveCmd, tCmd)
}

func (tCmd *TopologyCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(tCmd.Rpc.Info, &tCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	tCmd.Error = errCmd
	topologyResponse := response.(*topology.ListTopologyResponse)
	tCmd.Rpc.Response = topologyResponse
	tCmd.updateMetaserverAddr(topologyResponse.GetMetaservers().MetaServerInfos)
	topologyMap, topoErr := curveutil.Topology2Map(topologyResponse)
	tCmd.Error = topoErr
	tCmd.updateTable(&topologyMap)
	tCmd.Result = topologyMap

	return nil
}

func (tCmd *TopologyCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd)
}

func (tCmd *TopologyCommand) updateTable(topoMap *map[string]interface{}) *cmderror.CmdError {
	errs := make([]*cmderror.CmdError, 0)
	poolList := (*topoMap)[curveutil.POOL_LIST].([]*curveutil.PoolInfo)
	sort.SliceStable(poolList, func(i, j int) bool {
		return *poolList[i].PoolID <
			*poolList[j].PoolID
	})
	for _, pool := range poolList {
		poolStr := *pool.PoolName
		sort.SliceStable(pool.Zones, func(i, j int) bool {
			return pool.Zones[i].GetZoneID() <
				pool.Zones[j].GetZoneID()
		})
		for _, zone := range pool.Zones {
			zoneStr := zone.GetZoneName()
			sort.SliceStable(zone.Servers, func(i, j int) bool {
				return zone.Servers[i].GetServerID() <
					zone.Servers[j].GetServerID()
			})
			for _, server := range zone.Servers {
				serverStr := server.GetHostName()
				sort.SliceStable(server.Metaservers, func(i, j int) bool {
					return server.Metaservers[i].GetMetaServerID() <
						server.Metaservers[j].GetMetaServerID()
				})
				for _, metaserver := range server.Metaservers {
					metaserverStr := fmt.Sprintf("%s:%d", metaserver.GetExternalIp(), metaserver.GetExternalPort())
					row := []string{poolStr, zoneStr, serverStr, metaserverStr}
					tCmd.TableNew.Append(row)
				}
				if len(server.Metaservers) == 0 {
					row := []string{poolStr, zoneStr, serverStr, ""}
					tCmd.TableNew.Append(row)
				}
			}
			if len(zone.Servers) == 0 {
				row := []string{poolStr, zoneStr, "", ""}
				tCmd.TableNew.Append(row)
			}
		}
		if len(pool.Zones) == 0 {
			row := []string{poolStr, "", "", ""}
			tCmd.TableNew.Append(row)
		}
	}
	retErr := cmderror.MergeCmdError(errs)
	return retErr
}

func (tCmd *TopologyCommand) updateMetaserverAddr(metaservers []*topology.MetaServerInfo) {
	for _, metaserver := range metaservers {
		internalAddr := fmt.Sprintf("%s:%d", metaserver.GetInternalIp(), metaserver.GetInternalPort())
		tCmd.internalAddr = append(tCmd.internalAddr, internalAddr)

		externalAddr := fmt.Sprintf("%s:%d", metaserver.GetExternalIp(), metaserver.GetExternalPort())
		tCmd.externalAddr = append(tCmd.externalAddr, externalAddr)
	}
}
