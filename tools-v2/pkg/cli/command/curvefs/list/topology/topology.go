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
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
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

func GetMetaserverAddrs() ([]string, []string, *cmderror.CmdError) {
	listTopo := NewListTopologyCommand()
	listTopo.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	listTopo.Cmd.SilenceErrors = true
	err := listTopo.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetMetaserverAddr()
		retErr.Format(err.Error())
		return nil, nil, retErr
	}
	return listTopo.externalAddr, listTopo.internalAddr, cmderror.ErrSuccess()
}

func GetTopology() (*topology.ListTopologyResponse, *cmderror.CmdError) {
	listTopo := NewListTopologyCommand()
	listTopo.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
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

	table, err := gotable.Create(cobrautil.ROW_ID, cobrautil.ROW_TYPE, cobrautil.ROW_NAME, cobrautil.ROW_CHILD_TYPE, cobrautil.ROW_CHILD_LIST)
	header := []string{cobrautil.ROW_ID, cobrautil.ROW_TYPE, cobrautil.ROW_NAME, cobrautil.ROW_CHILD_TYPE, cobrautil.ROW_CHILD_LIST}
	tCmd.SetHeader(header)
	var mergeIndex []int
	mergeRow := []string {cobrautil.ROW_TYPE, cobrautil.ROW_CHILD_TYPE}
	for _, row := range mergeRow {
		index := slices.Index(header, row)
		mergeIndex = append(mergeIndex, index)
	}
	tCmd.TableNew.SetAutoMergeCellsByColumnIndex(mergeIndex)
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
	tCmd.Rpc.Response = topologyResponse
	res, err := output.MarshalProtoJson(topologyResponse)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	tCmd.Result = mapRes
	tCmd.updateTable(tCmd.Table, topologyResponse)
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

func (tCmd *TopologyCommand) updateTable(table *table.Table, topology *topology.ListTopologyResponse) {
	pools := topology.GetPools().GetPoolInfos()
	poolRows := make([]map[string]string, 0)
	for _, pool := range pools {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(pool.GetPoolID()), 10)
		row[cobrautil.ROW_ID] = id
		row[cobrautil.ROW_TYPE] = "pool"
		row[cobrautil.ROW_NAME] = pool.GetPoolName()
		row[cobrautil.ROW_CHILD_TYPE] = "zone"
		row[cobrautil.ROW_CHILD_LIST] = ""
		poolRows = append(poolRows, row)
	}

	zones := topology.GetZones().GetZoneInfos()
	zoneRows := make([]map[string]string, 0)
	for _, zone := range zones {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(zone.GetZoneID()), 10)
		row[cobrautil.ROW_ID] = id
		row[cobrautil.ROW_TYPE] = "zone"
		row[cobrautil.ROW_NAME] = zone.GetZoneName()
		row[cobrautil.ROW_CHILD_TYPE] = "server"
		row[cobrautil.ROW_CHILD_LIST] = ""
		zoneRows = append(zoneRows, row)
		// update pools child list
		zonePoolId := strconv.FormatUint(uint64(zone.GetPoolID()), 10)
		for _, pool := range poolRows {
			if pool[cobrautil.ROW_ID] == zonePoolId {
				pool[cobrautil.ROW_CHILD_LIST] = pool[cobrautil.ROW_CHILD_LIST] + zone.GetZoneName() + " "
			}
		}
	}

	servers := topology.GetServers().GetServerInfos()
	serverRows := make([]map[string]string, 0)
	for _, server := range servers {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(server.GetServerID()), 10)
		row[cobrautil.ROW_ID] = id
		row[cobrautil.ROW_TYPE] = "server"
		row[cobrautil.ROW_NAME] = server.GetHostName()
		row[cobrautil.ROW_CHILD_TYPE] = "metaserver"
		row[cobrautil.ROW_CHILD_LIST] = ""
		serverRows = append(serverRows, row)
		// update pools child list
		serverZoneId := strconv.FormatUint(uint64(server.GetZoneID()), 10)
		for _, zone := range zoneRows {
			if zone[cobrautil.ROW_ID] == serverZoneId {
				zone[cobrautil.ROW_CHILD_LIST] = zone[cobrautil.ROW_CHILD_LIST] + server.GetHostName() + " "
			}
		}
	}

	metaservers := topology.GetMetaservers().GetMetaServerInfos()
	metaserverRows := make([]map[string]string, 0)
	for _, metaserver := range metaservers {
		row := make(map[string]string)
		id := strconv.FormatUint(uint64(metaserver.GetMetaServerID()), 10)
		row[cobrautil.ROW_ID] = id
		row[cobrautil.ROW_TYPE] = "metaserver"
		row[cobrautil.ROW_NAME] = metaserver.GetHostname()
		row[cobrautil.ROW_CHILD_TYPE] = ""
		row[cobrautil.ROW_CHILD_LIST] = ""
		metaserverRows = append(metaserverRows, row)
		// update server child list
		metaserverServerId := strconv.FormatUint(uint64(metaserver.GetServerId()), 10)
		for _, server := range serverRows {
			if server[cobrautil.ROW_ID] == metaserverServerId {
				server[cobrautil.ROW_CHILD_LIST] = server[cobrautil.ROW_CHILD_LIST] + fmt.Sprintf("%s.%d ", metaserver.GetHostname(), metaserver.GetMetaServerID())
			}
		}
	}

	rows := make([]map[string]string, 0)
	rows = append(rows, poolRows...)
	rows = append(rows, zoneRows...)
	rows = append(rows, serverRows...)
	rows = append(rows, metaserverRows...)
	table.AddRows(rows)
	list := cobrautil.ListMap2ListSortByKeys(rows, tCmd.Header, []string{})
	tCmd.TableNew.AppendBulk(list)
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
