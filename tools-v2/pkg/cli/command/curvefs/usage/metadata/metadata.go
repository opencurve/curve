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
 * Created Date: 2022-06-13
 * Author: chengyi (Cyber-SiKu)
 */

package metadata

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type MetadataRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.StatMetadataUsageRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*MetadataRpc)(nil) // check interface

type MetadataCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *MetadataRpc
	response *topology.StatMetadataUsageResponse
}

var _ basecmd.FinalCurveCmdFunc = (*MetadataCommand)(nil) // check interface

func (mRpc *MetadataRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	mRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (mRpc *MetadataRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return mRpc.topologyClient.StatMetadataUsage(ctx, mRpc.Request)
}

const (
	metadataExample = `$ curvefs fs usage metadata`
)

func NewMetadataCommand() *cobra.Command {
	fsCmd := &MetadataCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "metadata",
			Short:   "get the usage of metadata in curvefs",
			Example: metadataExample,
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (mCmd *MetadataCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(mCmd.Cmd)
	config.AddRpcTimeoutFlag(mCmd.Cmd)
	config.AddFsMdsAddrFlag(mCmd.Cmd)
}

func (mCmd *MetadataCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(mCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	mCmd.Rpc = &MetadataRpc{}
	mCmd.Rpc.Request = &topology.StatMetadataUsageRequest{}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	mCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "StatMetadataUsage")

	header := []string{cobrautil.ROW_METASERVER_ADDR, cobrautil.ROW_TOTAL, cobrautil.ROW_USED, cobrautil.ROW_LEFT}
	mCmd.SetHeader(header)

	return nil
}

func (mCmd *MetadataCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mCmd.FinalCurveCmd, mCmd)
}

func (mCmd *MetadataCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(mCmd.Rpc.Info, mCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	mCmd.Error = errCmd
	mCmd.response = response.(*topology.StatMetadataUsageResponse)
	res, err := output.MarshalProtoJson(mCmd.response)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	// update uint
	data := mapRes["metadataUsages"].([]interface{})
	for _, v := range data {
		vm := v.(map[string]interface{})
		vm["uint"] = "Byte"
	}
	mCmd.Result = mapRes
	mCmd.updateTable()
	return nil
}

func (mCmd *MetadataCommand) updateTable() {
	rows := make([]map[string]string, 0)
	for _, md := range mCmd.response.GetMetadataUsages() {
		row := make(map[string]string)
		row[cobrautil.ROW_METASERVER_ADDR] = md.GetMetaserverAddr()
		row[cobrautil.ROW_TOTAL] = humanize.IBytes(md.GetTotal())
		row[cobrautil.ROW_USED] = humanize.IBytes(md.GetUsed())
		row[cobrautil.ROW_LEFT] = humanize.IBytes(md.GetTotal() - md.GetUsed())
		rows = append(rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, mCmd.Header, []string{})
	mCmd.TableNew.AppendBulk(list)
}

func (mCmd *MetadataCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&mCmd.FinalCurveCmd)
}
