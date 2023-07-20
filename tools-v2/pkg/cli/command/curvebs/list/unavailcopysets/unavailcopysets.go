// /*
//  *  Copyright (c) 2023 NetEase Inc.
//  *
//  *  Licensed under the Apache License, Version 2.0 (the "License");
//  *  you may not use this file except in compliance with the License.
//  *  You may obtain a copy of the License at
//  *
//  *      http://www.apache.org/licenses/LICENSE-2.0
//  *
//  *  Unless required by applicable law or agreed to in writing, software
//  *  distributed under the License is distributed on an "AS IS" BASIS,
//  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  *  See the License for the specific language governing permissions and
//  *  limitations under the License.
//  */

// /*
//  * Project: CurveCli
//  * Created Date: 2023-04-24
//  * Author: baytan
//  */

package unavailcopysets

// import (
// 	"context"

// 	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
// 	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
// 	"github.com/opencurve/curve/tools-v2/pkg/config"
// 	"github.com/opencurve/curve/tools-v2/pkg/output"
// 	common "github.com/opencurve/curve/tools-v2/proto/proto/common"
// 	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
// 	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
// 	"github.com/spf13/cobra"
// 	"google.golang.org/grpc"
// )

// type ListUnAvailCopySets struct {
// 	Info           *basecmd.Rpc
// 	Request        *topology.ListUnAvaUnilCopySetsRequest
// 	topologyClient topology.TopologyServiceClient
// }

// var _ basecmd.RpcFunc = (*ListUnAvailCopySets)(nil) // check interface

// func (lRpc *ListUnAvailCopySets) NewRpcClient(cc grpc.ClientConnInterface) {
// 	lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
// }

// func (lRpc *ListUnAvailCopySets) Stub_Func(ctx context.Context) (interface{}, error) {
// 	return lRpc.topologyClient.ListUnAvailCopySets(ctx, lRpc.Request)
// }

// type UnAvailCopySetsCommand struct {
// 	basecmd.FinalCurveCmd
// 	Rpc      *ListUnAvailCopySets
// 	response []*common.CopysetInfo
// }

// var _ basecmd.FinalCurveCmdFunc = (*UnAvailCopySetsCommand)(nil) // check interface

// func NewUnAvailCopySetsCommand() *cobra.Command {
// 	return NewListUnAvailCopySetsCommand().Cmd
// }

// func NewListUnAvailCopySetsCommand() *UnAvailCopySetsCommand {
// 	uCmd := &UnAvailCopySetsCommand{
// 		FinalCurveCmd: basecmd.FinalCurveCmd{},
// 	}

// 	basecmd.NewFinalCurveCli(&uCmd.FinalCurveCmd, uCmd)
// 	return uCmd
// }

// func (uCmd *UnAvailCopySetsCommand) AddFlags() {
// 	config.AddBsMdsFlagOption(uCmd.Cmd)
// 	config.AddRpcRetryTimesFlag(uCmd.Cmd)
// 	config.AddRpcTimeoutFlag(uCmd.Cmd)
// }

// func (uCmd *UnAvailCopySetsCommand) Init(cmd *cobra.Command, args []string) error {
// 	mdsAddrs, err := config.GetBsMdsAddrSlice(uCmd.Cmd)
// 	if err.TypeCode() != cmderror.CODE_SUCCESS {
// 		return err.ToError()
// 	}
// 	timeout := config.GetFlagDuration(uCmd.Cmd, config.RPCTIMEOUT)
// 	retrytimes := config.GetFlagInt32(uCmd.Cmd, config.RPCRETRYTIMES)
// 	uCmd.Rpc = &ListUnAvailCopySets{
// 		Request: &topology.ListUnAvailCopySetsRequest{},
// 		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListUnAvailCopySets"),
// 	}
// 	return nil
// }

// func (uCmd *UnAvailCopySetsCommand) Print(cmd *cobra.Command, args []string) error {
// 	return output.FinalCmdOutput(&uCmd.FinalCurveCmd, uCmd)
// }

// func (uCmd *UnAvailCopySetsCommand) RunCommand(cmd *cobra.Command, args []string) error {
// 	result, errCmd := basecmd.GetRpcResponse(uCmd.Rpc.Info, uCmd.Rpc)
// 	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
// 		return errCmd.ToError()
// 	}
// 	response := result.(*topology.ListUnAvailCopySetsResponse)
// 	if response.StatusCode == nil ||
// 		response.GetStatusCode() != int32(statuscode.TopoStatusCode_Success) {
// 		code := statuscode.TopoStatusCode(response.GetStatusCode())
// 		return cmderror.ErrBsListPhysicalPoolRpc(code).ToError()
// 	}
// 	uCmd.response = response.Copysets
// 	return nil
// }

// func (uCmd *UnAvailCopySetsCommand) ResultPlainOutput() error {
// 	return output.FinalCmdOutputPlain(&uCmd.FinalCurveCmd)
// }

// func GetUnAvailCopySets(caller *cobra.Command) ([]*common.CopysetInfo, *cmderror.CmdError) {
// 	listUnAvailCopySets := NewListUnAvailCopySetsCommand()
// 	listUnAvailCopySets.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
// 	listUnAvailCopySets.Cmd.SilenceErrors = true
// 	config.AlignFlagsValue(caller, listUnAvailCopySets.Cmd, []string{
// 		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
// 	})
// 	err := listUnAvailCopySets.Cmd.Execute()
// 	if err != nil {
// 		retErr := cmderror.ErrBsGetUnavailCopysets()
// 		retErr.Format(err.Error())
// 		return nil, retErr
// 	}
// 	return listUnAvailCopySets.response, cmderror.ErrSuccess()
// }
