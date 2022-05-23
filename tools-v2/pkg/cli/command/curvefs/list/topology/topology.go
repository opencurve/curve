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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/liushuochen/gotable"
	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	topology "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type ListTopologyRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListTopologyRequest
	topologyClient topology.TopologyServiceClient
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
	topologyCmd := &TopologyCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "topology",
			Short: "list the topology of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&topologyCmd.FinalCurveCmd, topologyCmd)
	return topologyCmd.Cmd
}

func GetMetaserverAddrs() ([]string, []string, *cmderror.CmdError) {
	listTopo := NewListTopologyCommand()
	listTopo.Cmd.SetArgs([]string{"--format", "noout"})
	err := listTopo.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetMetaserverAddr()
		retErr.Format(err.Error())
		return nil, nil, retErr
	}
	return listTopo.externalAddr, listTopo.internalAddr, cmderror.ErrSuccess()
}

func NewListTopologyCommand() *TopologyCommand {
	topologyCmd := &TopologyCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
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

	table, err := gotable.Create("id", "type", "name", "child type", "child list")
	if err != nil {
		return err
	}
	tCmd.Table = table
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
	res, err := output.MarshalProtoJson(topologyResponse)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	tCmd.Result = mapRes
	updateTable(tCmd.Table, topologyResponse)
	errs := updateJsonPoolInfoRedundanceAndPlaceMentPolicy(&mapRes, topologyResponse)
	if len(errs) > 0 {
		return fmt.Errorf(cmderror.MostImportantCmdError(errs).Message)
	}

	tCmd.updateMetaserverAddr(topologyResponse.GetMetaservers().MetaServerInfos)
	return nil
}

func (tCmd *TopologyCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd, tCmd)
}

func updateTable(table *table.Table, topology *topology.ListTopologyResponse) {
	pools := topology.GetPools().GetPoolInfos()
	rows := make([]map[string]string, 0)
	for _, pool := range pools {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(pool.GetPoolID()), 10)
		row["id"] = id
		row["type"] = "pool"
		row["name"] = pool.GetPoolName()
		row["child type"] = "zone"
		row["child list"] = ""
		rows = append(rows, row)
	}
	poolRows := rows

	zones := topology.GetZones().GetZoneInfos()
	zoneRows := make([]map[string]string, 0)
	for _, zone := range zones {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(zone.GetZoneID()), 10)
		row["id"] = id
		row["type"] = "zone"
		row["name"] = zone.GetZoneName()
		row["child type"] = "server"
		row["child list"] = ""
		rows = append(rows, row)
		zoneRows = append(zoneRows, row)
		// update pools child list
		zonePoolId := strconv.FormatUint(uint64(zone.GetPoolID()), 10)
		for _, pool := range poolRows {
			if pool["id"] == zonePoolId {
				pool["child list"] = pool["child list"] + zone.GetZoneName() + " "
			}
		}
	}

	servers := topology.GetServers().GetServerInfos()
	serverRows := make([]map[string]string, 0)
	for _, server := range servers {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(server.GetServerID()), 10)
		row["id"] = id
		row["type"] = "server"
		row["name"] = server.GetHostName()
		row["child type"] = "metaserver"
		row["child list"] = ""
		rows = append(rows, row)
		serverRows = append(serverRows, row)
		// update pools child list
		serverZoneId := strconv.FormatUint(uint64(server.GetZoneID()), 10)
		for _, zone := range zoneRows {
			if zone["id"] == serverZoneId {
				zone["child list"] = zone["child list"] + server.GetHostName() + " "
			}
		}
	}

	metaservers := topology.GetMetaservers().GetMetaServerInfos()
	for _, metaserver := range metaservers {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(metaserver.GetMetaServerID()), 10)
		row["id"] = id
		row["type"] = "metaserver"
		row["name"] = metaserver.GetHostname()
		row["child type"] = ""
		row["child list"] = ""
		rows = append(rows, row)
		// update server child list
		metaserverServerId := strconv.FormatUint(uint64(metaserver.GetServerId()), 10)
		for _, server := range serverRows {
			if server["id"] == metaserverServerId {
				server["child list"] = server["child list"] + fmt.Sprintf("%s.%d ", metaserver.GetHostname(), metaserver.GetMetaServerID())
			}
		}
	}

	table.AddRows(rows)
}

func (tCmd *TopologyCommand) updateMetaserverAddr(metaservers []*topology.MetaServerInfo) {
	for _, metaserver := range metaservers {
		internalAddr := fmt.Sprintf("%s:%d", metaserver.GetInternalIp(), metaserver.GetInternalPort())
		tCmd.internalAddr = append(tCmd.internalAddr, internalAddr)

		externalAddr := fmt.Sprintf("%s:%d", metaserver.GetExternalIp(), metaserver.GetExternalPort())
		tCmd.externalAddr = append(tCmd.externalAddr, externalAddr)
	}
}

func updateJsonPoolInfoRedundanceAndPlaceMentPolicy(topologyMap *map[string]interface{}, topology *topology.ListTopologyResponse) []*cmderror.CmdError {
	var retErr []*cmderror.CmdError
	pools := (*topologyMap)["pools"]
	poolsInfoMap := pools.(map[string]interface{})["PoolInfos"].([]interface{})
	poolsInfoMapTopo := topology.GetPools().GetPoolInfos()
	for _, pool := range poolsInfoMap {
		poolInfo := pool.(map[string]interface{})
		var policyJson []byte
		for _, poolInfoTopo := range poolsInfoMapTopo {
			if uint32(poolInfo["PoolID"].(float64)) == poolInfoTopo.GetPoolID() {
				policyJson = poolInfoTopo.GetRedundanceAndPlaceMentPolicy()
				break
			}
		}
		var policy interface{}
		err := json.Unmarshal(policyJson, &policy)
		if err != nil {
			unmarshalErr := cmderror.ErrUnmarshalJson()
			unmarshalErr.Format(policyJson, err.Error())
			retErr = append(retErr, unmarshalErr)
			continue
		}
		poolInfo["redundanceAndPlaceMentPolicy"] = policy
	}
	return retErr
}
