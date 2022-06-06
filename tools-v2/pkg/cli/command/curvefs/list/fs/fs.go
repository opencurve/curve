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
 * Created Date: 2022-06-09
 * Author: chengyi (Cyber-SiKu)
 */

package fs

import (
	"context"
	"fmt"
	"strings"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type ListFsRpc struct {
	Info      basecmd.Rpc
	Request   *mds.ListClusterFsInfoRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*ListFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc ListFsRpc
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (fRpc *ListFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	fRpc.mdsClient = mds.NewMdsServiceClient(cc)
}

func (fRpc *ListFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return fRpc.mdsClient.ListClusterFsInfo(ctx, fRpc.Request)
}

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "fs",
			Short: "list all fs info of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (fCmd *FsCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddFsMdsAddrFlag(fCmd.Cmd)
}

func (fCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	hosts := viper.GetString(config.VIPER_CURVEFS_MDSADDR)
	addrs := strings.Split(hosts, ",")
	for _, addr := range addrs {
		if !cobrautil.IsValidAddr(addr) {
			return fmt.Errorf("invalid addr: %s", addr)
		}
	}
	fCmd.Rpc.Request = &mds.ListClusterFsInfoRequest{}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fCmd.Rpc.Info = *basecmd.NewRpc(addrs, timeout, retrytimes, "ListTopology")

	table, err := gotable.Create("id", "type", "name", "child type", "child list")
	if err != nil {
		return err
	}
	fCmd.Table = table
	return nil
}

func (fCmd *FsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	response, errs := basecmd.GetRpcResponse(fCmd.Rpc.Info, &fCmd.Rpc)
	errCmd := cmderror.MostImportanfCmdError(errs)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	topologyResponse := response.(*topology.ListTopologyResponse)
	res, err := output.MarshalProtoJson(topologyResponse)
	if err != nil {
		return err
	}
	mapRes := res.(map[string]interface{})
	fCmd.Result = mapRes

	return nil
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd, fCmd)
}
