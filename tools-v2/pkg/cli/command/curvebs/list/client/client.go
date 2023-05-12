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
 * Created Date: 2022-11-10
 * Author: Tsonglew
 */

package client

import (
	"context"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	clientExample = `$ curve bs list client`
)

type ListClientRpc struct {
	Info          *basecmd.Rpc
	Request       *nameserver2.ListClientRequest
	curveFSClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*ListClientRpc)(nil) // check interface

type ClientCommand struct {
	basecmd.FinalCurveCmd
	Rpc []*ListClientRpc
}

var _ basecmd.FinalCurveCmdFunc = (*ClientCommand)(nil) // check interface

func (lRpc *ListClientRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.curveFSClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (lRpc *ListClientRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.curveFSClient.ListClient(ctx, lRpc.Request)
}

func NewClientCommand() *cobra.Command {
	return NewListCLientCommand().Cmd
}

func NewListCLientCommand() *ClientCommand {
	lsCmd := &ClientCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "client",
			Short:   "list all client information in curvebs",
			Example: clientExample,
		},
	}

	basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
	return lsCmd
}

// AddFlags implements basecmd.FinalCurveCmdFunc
func (pCmd *ClientCommand) AddFlags() {
	config.AddBsMdsFlagOption(pCmd.Cmd)
	config.AddRpcRetryTimesFlag(pCmd.Cmd)
	config.AddRpcTimeoutFlag(pCmd.Cmd)
}

// Init implements basecmd.FinalCurveCmdFunc
func (pCmd *ClientCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(pCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	timeout := config.GetFlagDuration(pCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(pCmd.Cmd, config.RPCRETRYTIMES)

	rpc := &ListClientRpc{
		Request: &nameserver2.ListClientRequest{},
		Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "ListClient"),
	}
	pCmd.Rpc = append(pCmd.Rpc, rpc)

	header := []string{cobrautil.ROW_IP, cobrautil.ROW_PORT}
	pCmd.SetHeader(header)
	pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		pCmd.Header, header,
	))
	return nil
}

// Print implements basecmd.FinalCurveCmdFunc
func (pCmd *ClientCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

// RunCommand implements basecmd.FinalCurveCmdFunc
func (pCmd *ClientCommand) RunCommand(cmd *cobra.Command, args []string) error {
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
	var errors []*cmderror.CmdError
	rows := make([]map[string]string, 0)
	for _, res := range results {
		if res == nil {
			continue
		}
		infos := res.(*nameserver2.ListClientResponse).GetClientInfos()
		for _, info := range infos {
			row := make(map[string]string)
			row[cobrautil.ROW_IP] = info.GetIp()
			row[cobrautil.ROW_PORT] = fmt.Sprintf("%d", info.GetPort())
			rows = append(rows, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string{
		cobrautil.ROW_IP,
	})
	pCmd.TableNew.AppendBulk(list)
	errRet := cmderror.MergeCmdError(errors)
	pCmd.Error = errRet
	pCmd.Result = rows
	return nil
}

// ResultPlainOutput implements basecmd.FinalCurveCmdFunc
func (pCmd *ClientCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&pCmd.FinalCurveCmd)
}

func GetClientList(caller *cobra.Command) (*interface{}, *cmderror.CmdError) {
	listClientCmd := NewListCLientCommand()
	listClientCmd.Cmd.SetArgs([]string{
		fmt.Sprintf("--%s", config.FORMAT), config.FORMAT_NOOUT,
	})
	config.AlignFlagsValue(caller, listClientCmd.Cmd, []string{config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR})
	listClientCmd.Cmd.SilenceErrors = true
	err := listClientCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetClientList()
		retErr.Format(err.Error())
		return nil, retErr
	}
	return &listClientCmd.Result, cmderror.Success()
}
