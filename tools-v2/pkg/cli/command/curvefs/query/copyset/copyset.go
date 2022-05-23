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
 * Created Date: 2022-06-22
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"context"
	"fmt"
	"strconv"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/heartbeat"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type QueryCopysetRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetCopysetsInfoRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*QueryCopysetRpc)(nil) // check interface

type CopysetCommand struct {
	basecmd.FinalCurveCmd
	Rpc         *QueryCopysetRpc
	Rows        []map[string]string
	key2Copyset map[uint64]*CopysetInfoStatus
}

type CopysetInfoStatus struct {
	Info   *heartbeat.CopySetInfo         `json:"info,omitempty"`
	Status []*copyset.CopysetStatusResponse `json:"status,omitempty"`
}

var _ basecmd.FinalCurveCmdFunc = (*CopysetCommand)(nil) // check interface

func (qcRpc *QueryCopysetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	qcRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (qcRpc *QueryCopysetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return qcRpc.topologyClient.GetCopysetsInfo(ctx, qcRpc.Request)
}

func NewCopysetCommand() *cobra.Command {
	copysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "copyset",
			Short: "query copysets in curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd.Cmd
}

func (cCmd *CopysetCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
	config.AddCopysetidSliceRequiredFlag(cCmd.Cmd)
	config.AddPoolidSliceRequiredFlag(cCmd.Cmd)
	config.AddDetailOptionFlag(cCmd.Cmd)
}

func (cCmd *CopysetCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(cCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	var poolids []uint32
	poolidsStr := config.GetFlagStringSlice(cCmd.Cmd, config.CURVEFS_POOLID)
	for _, poolidStr := range poolidsStr {
		poolid, _ := strconv.ParseUint(poolidStr, 10, 32)
		poolids = append(poolids, uint32(poolid))
	}

	var copysetids []uint32
	copysetidsStr := config.GetFlagStringSlice(cCmd.Cmd, config.CURVEFS_COPYSETID)
	for _, copysetidStr := range copysetidsStr {
		copysetid, _ := strconv.ParseUint(copysetidStr, 10, 32)
		copysetids = append(copysetids, uint32(copysetid))
	}

	if len(poolids) != len(copysetids) {
		return fmt.Errorf("%s and %s is must be in one-to-one correspondence", config.CURVEFS_POOLID, config.CURVEFS_COPYSETID)
	}

	table, err := gotable.Create("copyset key", "leader peer", "epoch")
	if err != nil {
		return err
	}

	cCmd.Table = table

	cCmd.Rows = make([]map[string]string, 0)
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	getRequest := &topology.GetCopysetsInfoRequest{}
	for i := range poolids {
		poolid := poolids[i]
		copysetid := copysetids[i]
		copysetKey := topology.CopysetKey{
			PoolId:    &poolid,
			CopysetId: &copysetid,
		}
		key := cobrautil.GetCopysetKey(uint64(poolid), uint64(copysetid))
		getRequest.CopysetKeys = append(getRequest.CopysetKeys, &copysetKey)

		row := make(map[string]string)
		row["copyset key"] = fmt.Sprintf("%d", key)
		row["leader peer"] = "DNE"
		row["epoch"] = "DNE"

		cCmd.Rows = append(cCmd.Rows, row)
	}
	cCmd.Rpc = &QueryCopysetRpc{
		Request: getRequest,
	}
	cCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetCopysetsInfo")
	cCmd.key2Copyset = make(map[uint64]*CopysetInfoStatus)

	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	var errs []*cmderror.CmdError
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		errs = append(errs, err)
	} else {
		response := result.(*topology.GetCopysetsInfoResponse)
		copysetValues := response.GetCopysetValues()
		for _, copysetValue := range copysetValues {
			if copysetValue.GetStatusCode() == topology.TopoStatusCode_TOPO_OK {
				copysetInfo := copysetValue.GetCopysetInfo()
				poolid := copysetInfo.GetPoolId()
				copysetid := copysetInfo.GetCopysetId()
				copysetKey := cobrautil.GetCopysetKey(uint64(poolid), uint64(copysetid))

				if cCmd.key2Copyset[copysetKey] == nil {
					cCmd.key2Copyset[copysetKey] = &CopysetInfoStatus{
						Info: copysetInfo,
					}
				}

				row := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool { return row["copyset key"] == fmt.Sprintf("%d", copysetKey) })
				cCmd.Rows[row]["leader peer"] = copysetInfo.GetLeaderPeer().String()
				cCmd.Rows[row]["epoch"] = fmt.Sprintf("%d", copysetInfo.GetEpoch())
			} else {
				err := cmderror.ErrGetCopysetsInfo(int(copysetValue.GetStatusCode()))
				errs = append(errs, err)
			}
		}

		detail := config.GetFlagBool(cCmd.Cmd, config.CURVEFS_DETAIL)
		if detail {
			statusErr := cCmd.UpdateCopysetsStatus(copysetValues)
			errs = append(errs, statusErr...)
		}
	}

	cCmd.Table.AddRows(cCmd.Rows)
	cCmd.Result = cCmd.key2Copyset
	cCmd.Error = cmderror.MostImportantCmdError(errs)
	return nil
}

func (cCmd *CopysetCommand) UpdateCopysetsStatus(values []*topology.CopysetValue) []*cmderror.CmdError {
	cCmd.Table.AddColumn("peer addr")
	cCmd.Table.AddColumn("status")
	cCmd.Table.AddColumn("state")
	cCmd.Table.AddColumn("term")
	cCmd.Table.AddColumn("readonly")
	for _, row := range cCmd.Rows {
		row["peer addr"] = "DNE"
		row["status"] = "DNE"
		row["state"] = "DNE"
		row["term"] = "DNE"
		row["readonly"] = "DNE"
	}

	addr2Request := make(map[string]*copyset.CopysetsStatusRequest)
	var ret []*cmderror.CmdError
	for _, value := range values {
		if value.GetStatusCode() == topology.TopoStatusCode_TOPO_OK {
			copysetInfo := value.GetCopysetInfo()
			peers := copysetInfo.GetPeers()
			for _, peer := range peers {
				addr, err := cobrautil.SplitPeerToAddr(peer.GetAddress())
				if err.TypeCode() != cmderror.CODE_SUCCESS {
					ret = append(ret, err)
				} else if addr2Request[addr] == nil {
					addr2Request[addr] = &copyset.CopysetsStatusRequest{}
				}
				poolid := copysetInfo.GetPoolId()
				copysetid := copysetInfo.GetCopysetId()
				copsetStatRequest := &copyset.CopysetStatusRequest{
					PoolId:    &poolid,
					CopysetId: &copysetid,
				}
				addr2Request[addr].Copysets = append(addr2Request[addr].Copysets, copsetStatRequest)

				copysetKey := cobrautil.GetCopysetKey(uint64(poolid), uint64(copysetid))
				rowIndex := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool { return row["copyset key"] == fmt.Sprintf("%d", copysetKey) })
				if rowIndex == -1 {
					errIndex := cmderror.ErrCopysetKey()
					errIndex.Format(copysetKey)
					ret = append(ret, errIndex)
					continue
				}
				row := cCmd.Rows[rowIndex]
				if row["peer addr"] != "DNE" {
					rowNew := make(map[string]string)
					rowNew["copyset key"] = row["copyset key"]
					rowNew["leader peer"] = row["leader peer"]
					rowNew["epoch"] = row["epoch"]
					rowNew["peer addr"] = addr
					cCmd.Rows = append(cCmd.Rows, rowNew)
				} else {
					row["peer addr"] = addr
				}
			}
		}
	}

	// update row & copysetInfoStatus
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	results := GetCopysetsStatus(&addr2Request, timeout, retrytimes)
	for _, result := range results {
		ret = append(ret, result.Error)
		copysets := result.Request.GetCopysets()
		copysetsStatus := result.Status.GetStatus()
		for i := range copysets {
			copyset := copysets[i]
			status := copysetsStatus[i]
			poolId := copyset.GetPoolId()
			copysetId := copyset.GetCopysetId()
			copysetKey := cobrautil.GetCopysetKey(uint64(poolId), uint64(copysetId))

			copysetInfoStatus := cCmd.key2Copyset[copysetKey]
			copysetInfoStatus.Status = append(copysetInfoStatus.Status, status)

			rowIndex := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool {
				peerAddr := row["peer addr"]
				addr := result.Addr
				return (row["copyset key"] == fmt.Sprintf("%d", copysetKey)) && (peerAddr == addr) 
			})

			if rowIndex == -1 {
				errIndex := cmderror.ErrCopysetKey()
				errIndex.Format(copysetKey)
				ret = append(ret, errIndex)
				continue
			}

			row := cCmd.Rows[rowIndex]
			row["status"] = status.GetStatus().String()
			copysetStatus := status.GetCopysetStatus()
			row["state"] = fmt.Sprintf("%d", copysetStatus.GetState())
			row["term"] = fmt.Sprintf("%d", copysetStatus.GetTerm())
			row["readonly"] = fmt.Sprintf("%t", copysetStatus.GetReadonly())

		}
	}
	return ret
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd, cCmd)
}
