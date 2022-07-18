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
 * Created Date: 2022-06-14
 * Author: chengyi (Cyber-SiKu)
 */

package partition

import (
	"context"
	"fmt"
	"strconv"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/fs"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

const (
	partitionExample = `$ curve fs list partition
$ curve fs list partition --fsid=1,2,3`
)

type ListPartitionRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPartitionRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListPartitionRpc)(nil) // check interface

type PartitionCommand struct {
	basecmd.FinalCurveCmd
	Rpc                []*ListPartitionRpc
	fsId2Rows          map[uint32][]map[string]string
	fsId2PartitionList map[uint32][]*common.PartitionInfo
}

var _ basecmd.FinalCurveCmdFunc = (*PartitionCommand)(nil) // check interface

func (lpRp *ListPartitionRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lpRp.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpRp *ListPartitionRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lpRp.topologyClient.ListPartition(ctx, lpRp.Request)
}

func NewPartitionCommand() *cobra.Command {
	pCmd := &PartitionCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "partition",
			Short:   "list partition in curvefs by fsid",
			Example: partitionExample,
		},
	}
	basecmd.NewFinalCurveCli(&pCmd.FinalCurveCmd, pCmd)
	return pCmd.Cmd
}

func (pCmd *PartitionCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
	config.AddFsMdsAddrFlag(pCmd.Cmd)
	config.AddFsIdOptionDefaultAllFlag(pCmd.Cmd)
}

func (pCmd *PartitionCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(pCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	table, err := gotable.Create(cobrautil.ROW_PARTITION_ID, cobrautil.ROW_FS_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID, cobrautil.ROW_START, cobrautil.ROW_END, cobrautil.ROW_STATUS)
	header := []string{cobrautil.ROW_PARTITION_ID, cobrautil.ROW_FS_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID, cobrautil.ROW_START, cobrautil.ROW_END, cobrautil.ROW_STATUS}
	pCmd.SetHeader(header)
	mergeRow := []string{cobrautil.ROW_FS_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID}
	var mergeIndex []int
	for _, row := range mergeRow {
		index := slices.Index(header, row)
		if index != -1 {
			mergeIndex = append(mergeIndex, index)
		}
	}
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(mergeIndex)

	if err != nil {
		return err
	}
	pCmd.Table = table
	fsIds := config.GetFlagStringSliceDefaultAll(pCmd.Cmd, config.CURVEFS_FSID)
	if fsIds[0] == "*" {
		var getFsIdErr *cmderror.CmdError
		fsIds, getFsIdErr = fs.GetFsIds(pCmd.Cmd)
		if getFsIdErr.TypeCode() != cmderror.CODE_SUCCESS {
			return fmt.Errorf(getFsIdErr.Message)
		}
	}
	pCmd.fsId2Rows = make(map[uint32][]map[string]string)
	pCmd.fsId2PartitionList = make(map[uint32][]*common.PartitionInfo)
	for _, fsId := range fsIds {
		id, err := strconv.ParseUint(fsId, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid fsId: %s", fsId)
		}
		request := &topology.ListPartitionRequest{}
		id32 := uint32(id)
		request.FsId = &id32
		rpc := &ListPartitionRpc{
			Request: request,
		}
		timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
		retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
		rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListPartition")
		pCmd.Rpc = append(pCmd.Rpc, rpc)
		pCmd.fsId2Rows[id32] = make([]map[string]string, 1)
		pCmd.fsId2Rows[id32][0] = make(map[string]string)
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_FS_ID] = fsId
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_POOL_ID] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_COPYSET_ID] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_PARTITION_ID] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_START] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_END] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2Rows[id32][0][cobrautil.ROW_STATUS] = cobrautil.ROW_VALUE_DNE
		pCmd.fsId2PartitionList[id32] = make([]*common.PartitionInfo, 0)
	}

	return nil
}

func (pCmd *PartitionCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PartitionCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	var resList []interface{}
	for _, result := range results {
		response := result.(*topology.ListPartitionResponse)
		res, err := output.MarshalProtoJson(response)
		if err != nil {
			errMar := cmderror.ErrMarShalProtoJson()
			errMar.Format(err.Error())
			errs = append(errs, errMar)
		}
		resList = append(resList, res)
		// update fsId2Rows
		partitionList := response.GetPartitionInfoList()
		for _, partition := range partitionList {
			fsId := partition.GetFsId()
			pCmd.fsId2PartitionList[fsId] = append(pCmd.fsId2PartitionList[fsId], partition)
			var row *map[string]string
			if len(pCmd.fsId2Rows[fsId]) == 1 && pCmd.fsId2Rows[fsId][0][cobrautil.ROW_POOL_ID] == cobrautil.ROW_VALUE_DNE {
				row = &pCmd.fsId2Rows[fsId][0]
				pCmd.fsId2Rows[fsId] = make([]map[string]string, 0)
			} else {
				temp := make(map[string]string)
				row = &temp
				(*row)[cobrautil.ROW_FS_ID] = strconv.FormatUint(uint64(fsId), 10)
			}
			(*row)[cobrautil.ROW_POOL_ID] = strconv.FormatUint(uint64(partition.GetPoolId()), 10)
			(*row)[cobrautil.ROW_COPYSET_ID] = strconv.FormatUint(uint64(partition.GetCopysetId()), 10)
			(*row)[cobrautil.ROW_PARTITION_ID] = strconv.FormatUint(uint64(partition.GetPartitionId()), 10)
			(*row)[cobrautil.ROW_START] = strconv.FormatUint(uint64(partition.GetStart()), 10)
			(*row)[cobrautil.ROW_END] = strconv.FormatUint(uint64(partition.GetEnd()), 10)
			(*row)[cobrautil.ROW_STATUS] = partition.GetStatus().String()
			pCmd.fsId2Rows[fsId] = append(pCmd.fsId2Rows[fsId], (*row))
		}
	}

	pCmd.updateTable()
	pCmd.Result = resList
	pCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (pCmd *PartitionCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *PartitionCommand) updateTable() {
	var total []map[string]string
	for _, rows := range pCmd.fsId2Rows {
		for _, row := range rows {
			pCmd.Table.AddRow(row)
			total = append(total, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(total, pCmd.Header, []string{
		cobrautil.ROW_FS_ID, cobrautil.ROW_POOL_ID, cobrautil.ROW_COPYSET_ID,
		cobrautil.ROW_START, cobrautil.ROW_PARTITION_ID, 
	})
	pCmd.TableNew.AppendBulk(list)
}

func NewListPartitionCommand() *PartitionCommand {
	pCmd := &PartitionCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "partition",
			Short: "list partition in curvefs by fsid",
		},
	}
	basecmd.NewFinalCurveCli(&pCmd.FinalCurveCmd, pCmd)
	return pCmd
}

func GetFsPartition(caller *cobra.Command) (*map[uint32][]*common.PartitionInfo, *cmderror.CmdError) {
	listPartionCmd := NewListPartitionCommand()
	listPartionCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	cobrautil.AlignFlagsValue(caller, listPartionCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEFS_MDSADDR,
		config.CURVEFS_FSID,
	})
	listPartionCmd.Cmd.SilenceErrors = true
	err := listPartionCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrGetFsPartition()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return &listPartionCmd.fsId2PartitionList, cmderror.ErrSuccess()
}
