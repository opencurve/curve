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
 * Created Date: 2022-06-29
 * Author: chengyi (Cyber-SiKu)
 */

package topology

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
)

const (
	topologyExample = `$ curve fs create topology --clustermap /path/to/clustermap.json`
)

type Topology struct {
	Servers []Server `json:"servers"`
	Zones   []Zone   `json:"-"`
	Pools   []Pool   `json:"pools"`
	PoolNum uint64   `json:"npools"`
}

type TopologyCommand struct {
	basecmd.FinalCurveCmd
	topology   Topology
	timeout    time.Duration
	retryTimes int32
	addrs      []string
	// pool
	clusterPoolsInfo []*topology.PoolInfo
	createPoolRpc    *CreatePoolRpc
	deletePoolRpc    *DeletePoolRpc
	listPoolRpc      *ListPoolRpc
	createPool       []*topology.CreatePoolRequest
	deletePool       []*topology.DeletePoolRequest
	// zone
	clusterZonesInfo []*topology.ZoneInfo
	deleteZoneRpc    *DeleteZoneRpc
	createZoneRpc    *CreateZoneRpc
	listPoolZoneRpc  *ListPoolZoneRpc
	createZone       []*topology.CreateZoneRequest
	deleteZone       []*topology.DeleteZoneRequest
	// server
	clusterServersInfo []*topology.ServerInfo
	deleteServerRpc    *DeleteServerRpc
	createServerRpc    *CreateServerRpc
	listZoneServerRpc  *ListZoneServerRpc
	createServer       []*topology.ServerRegistRequest
	deleteServer       []*topology.DeleteServerRequest
}

var _ basecmd.FinalCurveCmdFunc = (*TopologyCommand)(nil) // check interface

func NewTopologyCommand() *cobra.Command {
	topologyCmd := &TopologyCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "topology",
			Short:   "create curvefs topology",
			Example: topologyExample,
		},
	}
	basecmd.NewFinalCurveCli(&topologyCmd.FinalCurveCmd, topologyCmd)
	return topologyCmd.Cmd
}

func (tCmd *TopologyCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(tCmd.Cmd)
	config.AddRpcTimeoutFlag(tCmd.Cmd)
	config.AddFsMdsAddrFlag(tCmd.Cmd)
	config.AddClusterMapRequiredFlag(tCmd.Cmd)
}

func (tCmd *TopologyCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(tCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	tCmd.addrs = addrs
	tCmd.timeout = config.GetFlagDuration(tCmd.Cmd, config.RPCTIMEOUT)
	tCmd.retryTimes = config.GetFlagInt32(tCmd.Cmd, config.RPCRETRYTIMES)

	filePath := config.GetFlagString(tCmd.Cmd, config.CURVEFS_CLUSTERMAP)
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		jsonFileErr := cmderror.ErrReadFile()
		jsonFileErr.Format(filePath, err.Error())
		return jsonFileErr.ToError()
	}
	err = json.Unmarshal(byteValue, &tCmd.topology)
	if err != nil {
		jsonFileErr := cmderror.ErrReadFile()
		jsonFileErr.Format(filePath, err.Error())
		return jsonFileErr.ToError()
	}

	updateZoneErr := tCmd.updateZone()
	if updateZoneErr.TypeCode() != cmderror.CODE_SUCCESS {
		return updateZoneErr.ToError()
	}

	table, err := gotable.Create(cobrautil.ROW_NAME, cobrautil.ROW_TYPE, cobrautil.ROW_OPERATION, cobrautil.ROW_PARENT)
	if err != nil {
		return err
	}
	tCmd.Table = table

	scanErr := tCmd.scanCluster()
	if scanErr.TypeCode() != cmderror.CODE_SUCCESS {
		return scanErr.ToError()
	}

	return nil
}

func (tCmd *TopologyCommand) updateTopology() *cmderror.CmdError {
	// update cluster topology
	// remove
	// server
	err := tCmd.removeServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zone
	err = tCmd.removeZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// pool
	err = tCmd.removePools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	// create
	// pool
	err = tCmd.createPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zone
	err = tCmd.createZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zerver
	err = tCmd.createServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	return cmderror.ErrSuccess()
}

func (tCmd *TopologyCommand) RunCommand(cmd *cobra.Command, args []string) error {
	err := tCmd.updateTopology()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(err.Message)
	}
	result, resultErr := cobrautil.TableToResult(tCmd.Table)
	if resultErr != nil {
		return resultErr
	}
	tCmd.Result = result
	tCmd.Error = err
	return nil
}

func (tCmd *TopologyCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&tCmd.FinalCurveCmd, tCmd)
}

func (tCmd *TopologyCommand) ResultPlainOutput() error {
	if len(tCmd.createPool) == 0 && len(tCmd.deletePool) == 0 && len(tCmd.createZone) == 0 && len(tCmd.deleteZone) == 0 && len(tCmd.createServer) == 0 && len(tCmd.deleteServer) == 0 {
		fmt.Println("no change")
	}
	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd, tCmd)
}

// Compare the topology in the cluster with json,
// delete in the cluster not in json,
// create in json not in the topology.
func (tCmd *TopologyCommand) scanCluster() *cmderror.CmdError {
	err := tCmd.scanPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = tCmd.scanZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	err = tCmd.scanServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	return err
}
