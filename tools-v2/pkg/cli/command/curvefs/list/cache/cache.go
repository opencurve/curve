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
 * Created Date: 2022-10-21
 * Author: chengyi (Cyber-SiKu)
 */

package cache

import (
	"context"
	"fmt"

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

const (
	cacheExample = `$ curve fs list cacheCluster`
)

type ListCacheRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListMemcacheClusterRequest
	topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListCacheRpc)(nil) // check interface

type CacheCommand struct {
	basecmd.FinalCurveCmd
	Rpc *ListCacheRpc
}

var _ basecmd.FinalCurveCmdFunc = (*CacheCommand)(nil) // check interface

func (lRpc *ListCacheRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListCacheRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyClient.ListMemcacheCluster(ctx, lRpc.Request)
}

func NewCacheCommand() *cobra.Command {
	return NewListCacheCommand().Cmd
}

func NewListCacheCommand() *CacheCommand {
	cacheCmd := &CacheCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "cachecluster",
			Short:   "list all memcache cluster in the curvefs",
			Example: cacheExample,
		},
	}

	basecmd.NewFinalCurveCli(&cacheCmd.FinalCurveCmd, cacheCmd)
	return cacheCmd
}

func (cCmd *CacheCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
}

func (cCmd *CacheCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(cCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	cCmd.Rpc = &ListCacheRpc{
		Request: &topology.ListMemcacheClusterRequest{},
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	cCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "ListMemcacheCluster")

	header := []string{cobrautil.ROW_ID, cobrautil.ROW_SERVER}
	cCmd.SetHeader(header)
	cCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		cCmd.Header, []string{cobrautil.ROW_ID},
	))

	return nil
}

func (cCmd *CacheCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *CacheCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errCmd := basecmd.GetRpcResponse(cCmd.Rpc.Info, cCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	result := response.(*topology.ListMemcacheClusterResponse)
	res, err := output.MarshalProtoJson(result)
	if err != nil {
		return err
	}
	cCmd.Result = res
	cCmd.Error = cmderror.ErrListMemcacheCluster(result.GetStatusCode())

	rows := make([]map[string]string, 0)
	for _, cluster := range result.GetMemClusters() {
		for _, server := range cluster.GetServers() {
			row := make(map[string]string)
			row[cobrautil.ROW_ID] = fmt.Sprintf("%d", cluster.GetClusterId())
			row[cobrautil.ROW_SERVER] = fmt.Sprintf("%s:%d", server.GetIp(), server.GetPort())
			rows = append(rows, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, cCmd.Header, []string{cobrautil.ROW_ID})
	cCmd.TableNew.AppendBulk(list)
	return nil
}

func (cCmd *CacheCommand) ResultPlainOutput() error {
	if cCmd.TableNew.NumLines() == 0 {
		fmt.Println("no memcache Cluster in the curvefs")
	}
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
