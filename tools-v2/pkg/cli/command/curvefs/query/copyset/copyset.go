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

const (
	copysetExample = `$ curve fs query copyset --copysetid 1,2 --poolid 1,1
$ curve fs query copyset --copysetid 1,2 --poolid 1,1 --detail`
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
	key2Copyset map[uint64]*cobrautil.FsCopysetInfoStatus
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
			Use:     "copyset",
			Short:   "query copysets in curvefs",
			Example: copysetExample,
		},
	}
	basecmd.NewFinalCurveCli(&copysetCmd.FinalCurveCmd, copysetCmd)
	return copysetCmd.Cmd
}

func NewQueryCopysetCommand() *CopysetCommand {
	queryCopysetCmd := &CopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}
	basecmd.NewFinalCurveCli(&queryCopysetCmd.FinalCurveCmd, queryCopysetCmd)
	return queryCopysetCmd
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

	cCmd.Header = []string{cobrautil.ROW_COPYSET_KEY, cobrautil.ROW_COPYSET_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_LEADER_PEER, cobrautil.ROW_EPOCH}

	cCmd.Rows = make([]map[string]string, 0)
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	getRequest := &topology.GetCopysetsInfoRequest{}
	cCmd.key2Copyset = make(map[uint64]*cobrautil.FsCopysetInfoStatus)
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
		row[cobrautil.ROW_COPYSET_KEY] = fmt.Sprintf("%d", key)
		row[cobrautil.ROW_POOL_ID] = fmt.Sprintf("%d", poolid)
		row[cobrautil.ROW_COPYSET_ID] = fmt.Sprintf("%d", copysetid)
		row[cobrautil.ROW_LEADER_PEER] = cobrautil.ROW_VALUE_DNE
		row[cobrautil.ROW_EPOCH] = cobrautil.ROW_VALUE_DNE

		cCmd.Rows = append(cCmd.Rows, row)
		cCmd.key2Copyset[key] = nil
	}
	cCmd.Rpc = &QueryCopysetRpc{
		Request: getRequest,
	}
	cCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetCopysetsInfo")

	return nil
}

func (cCmd *CopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	var errs []*cmderror.CmdError
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(err.Message)
	} else {
		response := result.(*topology.GetCopysetsInfoResponse)
		copysetValues := response.GetCopysetValues()
		allNotFound := true
		for _, copysetValue := range copysetValues {
			var copysetKey uint64
			var copysetInfo *heartbeat.CopySetInfo
			if copysetValue.GetStatusCode() != topology.TopoStatusCode_TOPO_COPYSET_NOT_FOUND {
				allNotFound = false
				copysetInfo = copysetValue.GetCopysetInfo()
				poolid := copysetInfo.GetPoolId()
				copysetid := copysetInfo.GetCopysetId()
				copysetKey = cobrautil.GetCopysetKey(uint64(poolid), uint64(copysetid))
				if cCmd.key2Copyset[copysetKey] == nil {
					cCmd.key2Copyset[copysetKey] = &cobrautil.FsCopysetInfoStatus{
						Info: copysetInfo,
					}
				}
			}
			if copysetValue.GetStatusCode() == topology.TopoStatusCode_TOPO_OK {
				row := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool {
					return row[cobrautil.ROW_COPYSET_KEY] == fmt.Sprintf("%d", copysetKey)
				})
				cCmd.Rows[row][cobrautil.ROW_LEADER_PEER] = copysetInfo.GetLeaderPeer().String()
				cCmd.Rows[row][cobrautil.ROW_EPOCH] = fmt.Sprintf("%d", copysetInfo.GetEpoch())
			} else {
				err := cmderror.ErrGetCopysetsInfo(int(copysetValue.GetStatusCode()))
				errs = append(errs, err)
			}
		}

		detail := config.GetFlagBool(cCmd.Cmd, config.CURVEFS_DETAIL)
		if !allNotFound && detail {
			statusErr := cCmd.UpdateCopysetsStatus(copysetValues)
			errs = append(errs, statusErr...)
		} else {
			cCmd.SetHeader(cCmd.Header)
		}
	}

	indexSlice := cobrautil.GetIndexSlice(cCmd.Header, []string{
		cobrautil.ROW_POOL_ID, cobrautil.ROW_LEADER_PEER,
	})
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(indexSlice)

	list := cobrautil.ListMap2ListSortByKeys(cCmd.Rows, cCmd.Header, []string{
		cobrautil.ROW_POOL_ID, cobrautil.ROW_LEADER_PEER,
		cobrautil.ROW_COPYSET_ID,
	})
	cCmd.TableNew.AppendBulk(list)
	cCmd.Result = cCmd.key2Copyset
	cCmd.Error = cmderror.MostImportantCmdError(errs)
	return nil
}

func (cCmd *CopysetCommand) UpdateCopysetsStatus(values []*topology.CopysetValue) []*cmderror.CmdError {
	cCmd.Header = append(cCmd.Header, cobrautil.ROW_PEER_ADDR)
	cCmd.Header = append(cCmd.Header, cobrautil.ROW_STATUS)
	cCmd.Header = append(cCmd.Header, cobrautil.ROW_STATE)
	cCmd.Header = append(cCmd.Header, cobrautil.ROW_TERM)
	cCmd.Header = append(cCmd.Header, cobrautil.ROW_READONLY)
	cCmd.SetHeader(cCmd.Header)
	for _, row := range cCmd.Rows {
		row[cobrautil.ROW_PEER_ADDR] = cobrautil.ROW_VALUE_DNE
		row[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_DNE
		row[cobrautil.ROW_STATE] = cobrautil.ROW_VALUE_DNE
		row[cobrautil.ROW_TERM] = cobrautil.ROW_VALUE_DNE
		row[cobrautil.ROW_READONLY] = cobrautil.ROW_VALUE_DNE
	}

	addr2Request := make(map[string]*copyset.CopysetsStatusRequest)
	var ret []*cmderror.CmdError
	for _, value := range values {
		if value.GetStatusCode() == topology.TopoStatusCode_TOPO_OK {
			copysetInfo := value.GetCopysetInfo()
			peers := copysetInfo.GetPeers()
			for _, peer := range peers {
				addr, err := cobrautil.PeerAddressToAddr(peer.GetAddress())
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
				rowIndex := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool {
					return row[cobrautil.ROW_COPYSET_KEY] == fmt.Sprintf("%d", copysetKey)
				})
				if rowIndex == -1 {
					errIndex := cmderror.ErrCopysetKey()
					errIndex.Format(copysetKey, addr)
					ret = append(ret, errIndex)
					continue
				}
				row := cCmd.Rows[rowIndex]
				if row[cobrautil.ROW_PEER_ADDR] != cobrautil.ROW_VALUE_DNE {
					rowNew := make(map[string]string)
					rowNew[cobrautil.ROW_COPYSET_KEY] = row[cobrautil.ROW_COPYSET_KEY]
					rowNew[cobrautil.ROW_COPYSET_ID] = row[cobrautil.ROW_COPYSET_ID]
					rowNew[cobrautil.ROW_POOL_ID] = row[cobrautil.ROW_POOL_ID]
					rowNew[cobrautil.ROW_LEADER_PEER] = row[cobrautil.ROW_LEADER_PEER]
					rowNew[cobrautil.ROW_EPOCH] = row[cobrautil.ROW_EPOCH]
					rowNew[cobrautil.ROW_PEER_ADDR] = addr
					rowNew[cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_DNE
					rowNew[cobrautil.ROW_STATE] = cobrautil.ROW_VALUE_DNE
					rowNew[cobrautil.ROW_TERM] = cobrautil.ROW_VALUE_DNE
					rowNew[cobrautil.ROW_READONLY] = cobrautil.ROW_VALUE_DNE
					cCmd.Rows = append(cCmd.Rows, rowNew)
				} else {
					row[cobrautil.ROW_PEER_ADDR] = addr
				}
			}
		}
	}

	// update row & copysetInfoStatus
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	var results []*StatusResult
	if len(addr2Request) != 0 {
		results = GetCopysetsStatus(&addr2Request, timeout, retrytimes)
		for _, result := range results {
			ret = append(ret, result.Error)
			copysets := result.Request.GetCopysets()
			copysetsStatus := result.Status.GetStatus()
			for i := range copysets {
				ret = append(ret, result.Error)
				copysetInfo := copysets[i]
				poolId := copysetInfo.GetPoolId()
				copysetId := copysetInfo.GetCopysetId()
				copysetKey := cobrautil.GetCopysetKey(uint64(poolId), uint64(copysetId))

				copysetInfoStatus := cCmd.key2Copyset[copysetKey]
				var status *copyset.CopysetStatusResponse
				if copysetsStatus != nil {
					status = copysetsStatus[i]
				}
				if copysetInfoStatus.Peer2Status == nil {
					copysetInfoStatus.Peer2Status = make(map[string]*copyset.CopysetStatusResponse)
				}
				copysetInfoStatus.Peer2Status[result.Addr] = status

				rowIndex := slices.IndexFunc(cCmd.Rows, func(row map[string]string) bool {
					peerAddr := row[cobrautil.ROW_PEER_ADDR]
					addr := result.Addr
					return (row[cobrautil.ROW_COPYSET_KEY] == fmt.Sprintf("%d", copysetKey)) && (peerAddr == addr)
				})

				if rowIndex == -1 {
					errIndex := cmderror.ErrCopysetKey()
					errIndex.Format(copysetKey, result.Addr)
					ret = append(ret, errIndex)
					continue
				}

				row := cCmd.Rows[rowIndex]
				row[cobrautil.ROW_STATUS] = status.GetStatus().String()
				if status.GetStatus() != copyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_SUCCESS {
					row[cobrautil.ROW_STATE] = cobrautil.ROW_VALUE_DNE
					row[cobrautil.ROW_TERM] = cobrautil.ROW_VALUE_DNE
					row[cobrautil.ROW_READONLY] = cobrautil.ROW_VALUE_DNE
					continue
				}
				copysetStatus := status.GetCopysetStatus()
				row[cobrautil.ROW_STATE] = fmt.Sprintf("%d", copysetStatus.GetState())
				row[cobrautil.ROW_TERM] = fmt.Sprintf("%d", copysetStatus.GetTerm())
				row[cobrautil.ROW_READONLY] = fmt.Sprintf("%t", copysetStatus.GetReadonly())
			}
		}
	}
	return ret
}

func (cCmd *CopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

// copsetIds,poolId just like: 1,2,3
func QueryCopysetInfoStatus(caller *cobra.Command) (*map[uint64]*cobrautil.FsCopysetInfoStatus, *cmderror.CmdError) {
	queryCopyset := NewQueryCopysetCommand()
	queryCopyset.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.CURVEFS_DETAIL),
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, queryCopyset.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR, config.CURVEFS_COPYSETID, config.CURVEFS_POOLID})
	queryCopyset.Cmd.SilenceErrors = true
	err := queryCopyset.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrQueryCopyset()
		retErr.Format(err.Error())
		return nil, retErr
	}

	return &queryCopyset.key2Copyset, cmderror.ErrSuccess()
}

func QueryCopysetInfo(caller *cobra.Command) (*map[uint64]*cobrautil.FsCopysetInfoStatus, *cmderror.CmdError) {
	queryCopyset := NewQueryCopysetCommand()
	queryCopyset.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, queryCopyset.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR, config.CURVEFS_COPYSETID, config.CURVEFS_POOLID})
	queryCopyset.Cmd.SilenceErrors = true
	err := queryCopyset.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrQueryCopyset()
		retErr.Format(err.Error())
		return nil, retErr
	}

	return &queryCopyset.key2Copyset, cmderror.ErrSuccess()
}
