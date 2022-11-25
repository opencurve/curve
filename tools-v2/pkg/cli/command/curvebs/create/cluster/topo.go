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
 * Created Date: 2022-10-27
 * Author: ShiYue
 */

package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

const (
	deployExample = `$ curve bs create physicalpool --bsClusterMap $<filePath>`
)

type Topology struct {
	Servers       []Server       `json:"servers"`
	Zones         []Zone         `json:"-"`
	PhysicalPools []PhysicalPool `json:"-"`
	LogicalPools  []LogicalPool  `json:"logicalpools"`
	Poolsets      []Poolset      `json:"poolsets"`
}

type ClusterTopoCmd struct {
	basecmd.FinalCurveCmd
	topology   Topology
	timeout    time.Duration
	retryTimes int32
	addrs      []string

	//poolset
	clusterPoolsetsInfo []*topology.PoolsetInfo
	listPoolsetsRpc     *ListPoolsetsRpc
	delPoolsetRpc       *DelPoolsetRpc
	createPoolsetRpc    *CreatePoolsetRpc
	poolsetsToBeDeleted []*topology.PoolsetRequest
	poolsetsToBeCreated []*topology.PoolsetRequest

	// physicalpool
	clusterPhyPoolsInfo  []*topology.PhysicalPoolInfo
	listPhyPoolsInPstRpc *ListPhyPoolsInPstRpc
	delPhyPoolRpc        *DelPhyPoolRpc
	createPhyPoolRpc     *CreatePhyPoolRpc
	phyPoolsToBeCreated  []*topology.PhysicalPoolRequest
	phyPoolsToBeDeleted  []*topology.PhysicalPoolRequest

	//Zone
	clusterZonesInfo []*topology.ZoneInfo
	listZonesRpc     *ListZonesRpc
	delZoneRpc       *DelZoneRpc
	createZoneRpc    *CreateZoneRpc
	zonesToBeDeleted []*topology.ZoneRequest
	zonesToBeCreated []*topology.ZoneRequest

	//server
	clusterServersInfo []*topology.ServerInfo
	listServersRpc     *ListServersRpc
	delServerRpc       *DelServerRpc
	regServerRpc       *RegServerRpc
	serversToBeDeleted []*topology.DeleteServerRequest
	serversToBeReg     []*topology.ServerRegistRequest

	rows []map[string]string
}

func NewClusterTopoCmd() *cobra.Command {
	cluCmd := &ClusterTopoCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "physicalpool",
			Short:   "create physicalpools",
			Example: deployExample,
		},
	}
	basecmd.NewFinalCurveCli(&cluCmd.FinalCurveCmd, cluCmd)
	return cluCmd.Cmd
}

func InitRpcCall(ctCmd *ClusterTopoCmd) error {
	addrs, addrErr := config.GetBsMdsAddrSlice(ctCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	ctCmd.addrs = addrs
	ctCmd.timeout = config.GetBsFlagDuration(ctCmd.Cmd, config.RPCTIMEOUT)
	ctCmd.retryTimes = config.GetBsFlagInt32(ctCmd.Cmd, config.RPCRETRYTIMES)
	return nil
}

func (ctCmd *ClusterTopoCmd) AddFlags() {
	config.AddRpcRetryTimesFlag(ctCmd.Cmd)
	config.AddRpcTimeoutFlag(ctCmd.Cmd)
	config.AddBsMdsFlagOption(ctCmd.Cmd)
	config.AddBsClusterMapRequiredFlag(ctCmd.Cmd)
}

// read cluster map
func ReadClusterMap(ctCmd *ClusterTopoCmd) error {
	filePath := config.GetBsFlagString(ctCmd.Cmd, config.CURVEBS_CLUSTERMAP)
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
	err = json.Unmarshal(byteValue, &ctCmd.topology)
	if err != nil {
		jsonFileErr := cmderror.ErrReadFile()
		jsonFileErr.Format(filePath, err.Error())
		return jsonFileErr.ToError()
	}
	return nil
}

// Compare the topology in the cluster with json,
// delete in the cluster not in json,
// create in json not in the topology.
func (ctCmd *ClusterTopoCmd) scanCluster() *cmderror.CmdError {
	err := ctCmd.scanPoolsets()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = ctCmd.scanPhyPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	err = ctCmd.scanZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	err = ctCmd.scanServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	return err
}

func (ctCmd *ClusterTopoCmd) Init(cmd *cobra.Command, args []string) error {
	err := InitRpcCall(ctCmd)
	if err != nil {
		return err
	}
	err = ReadClusterMap(ctCmd)
	if err != nil {
		return err
	}
	genPhyPoolErr := ctCmd.genPhyPools()
	if genPhyPoolErr.TypeCode() != cmderror.CODE_SUCCESS {
		return genPhyPoolErr.ToError()
	}
	genZoneErr := ctCmd.genZones()
	if genZoneErr.TypeCode() != cmderror.CODE_SUCCESS {
		return genZoneErr.ToError()
	}

	header := []string{cobrautil.ROW_NAME, cobrautil.ROW_TYPE, cobrautil.ROW_OPERATION, cobrautil.ROW_PARENT}
	ctCmd.SetHeader(header)
	ctCmd.TableNew.SetAutoMergeCells(true)
	scanErr := ctCmd.scanCluster()
	if scanErr.TypeCode() != cmderror.CODE_SUCCESS {
		return scanErr.ToError()
	}

	return nil
}

func (ctCmd *ClusterTopoCmd) updateTopology() *cmderror.CmdError {
	// update cluster topology
	// remove
	// server
	err := ctCmd.removeServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zone
	err = ctCmd.removeZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// pool
	err = ctCmd.removePhyPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	//poolset
	err = ctCmd.removePoolsets()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	// create
	//poolset
	err = ctCmd.CreatePoolsets()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// pool
	err = ctCmd.CreatePhysicalPools()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zone
	err = ctCmd.CreateZones()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	// zerver
	err = ctCmd.RegistServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	return cmderror.ErrSuccess()
}

func (ctCmd *ClusterTopoCmd) RunCommand(cmd *cobra.Command, args []string) error {
	err := ctCmd.updateTopology()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		//return fmt.Errorf(err.Message)
		return err.ToError()
	}

	ctCmd.Result = ctCmd.rows
	ctCmd.Error = err
	return nil
}

func (ctCmd *ClusterTopoCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&ctCmd.FinalCurveCmd, ctCmd)
}

func (ctCmd *ClusterTopoCmd) ResultPlainOutput() error {
	if len(ctCmd.poolsetsToBeCreated) == 0 && len(ctCmd.poolsetsToBeDeleted) == 0 &&
		len(ctCmd.phyPoolsToBeCreated) == 0 && len(ctCmd.phyPoolsToBeDeleted) == 0 &&
		len(ctCmd.zonesToBeCreated) == 0 && len(ctCmd.zonesToBeDeleted) == 0 &&
		len(ctCmd.serversToBeReg) == 0 && len(ctCmd.serversToBeDeleted) == 0 {
		fmt.Println("no change")
		return nil
	}
	return output.FinalCmdOutputPlain(&ctCmd.FinalCurveCmd)
}
