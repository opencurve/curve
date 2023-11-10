/*
 *  Copyright (c) 2023 NetEase Inc.
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

package fs

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	fsExample = `$ curve fs update fs --fsname test1`
)

type UpdateFsRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.UpdateFsInfoRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*UpdateFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc *UpdateFsRpc
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (ufRp *UpdateFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	ufRp.mdsClient = mds.NewMdsServiceClient(cc)
}

func (ufRp *UpdateFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return ufRp.mdsClient.UpdateFsInfo(ctx, ufRp.Request)
}

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "fs",
			Short:   "update fsinfo",
			Long:    "update fsinfo",
			Example: fsExample,
		},
	}
	basecmd.NewFinalCurveCli(&fsCmd.FinalCurveCmd, fsCmd)
	return fsCmd.Cmd
}

func (fCmd *FsCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddFsMdsAddrFlag(fCmd.Cmd)
	config.AddFsNameRequiredFlag(fCmd.Cmd)
	// things can be changed
	config.AddCapacityOptionFlag(fCmd.Cmd)
}

func (fCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	// args check
	fsName, _ := cmd.Flags().GetString(config.CURVEFS_FSNAME)
	request := &mds.UpdateFsInfoRequest{
		FsName: &fsName,
	}

	nothingToChange := true
	var capacity string
	if config.GetFlagChanged(cmd, config.CURVEFS_CAPACITY) {
		nothingToChange = false
		capacity = config.GetFlagString(cmd, config.CURVEFS_CAPACITY)
		cap_bytes, _ := humanize.ParseBytes(capacity)
		request.Capacity = &cap_bytes
	}

	if nothingToChange {
		return fmt.Errorf("please specify something to change")
	}

	addrs, addrErr := config.GetFsMdsAddrSlice(fCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)

	// output format
	header := []string{cobrautil.ROW_FS_NAME, cobrautil.ROW_RESULT}
	fCmd.SetHeader(header)

	// set rpc
	fCmd.Rpc = &UpdateFsRpc{
		Request: request,
	}
	fCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "UpdateFsInfo")
	return nil
}

func (fCmd *FsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, errCmd := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}

	response := result.(*mds.UpdateFsInfoResponse)
	errUpdateFs := cmderror.ErrUpdateFs(int(response.GetStatusCode()))
	row := map[string]string{
		cobrautil.ROW_FS_NAME: fCmd.Rpc.Request.GetFsName(),
		cobrautil.ROW_RESULT:  errUpdateFs.Message,
	}

	fCmd.TableNew.Append(cobrautil.Map2List(row, fCmd.Header))

	var errs []*cmderror.CmdError
	res, errTranslate := output.MarshalProtoJson(response)
	if errTranslate != nil {
		errMar := cmderror.ErrMarShalProtoJson()
		errMar.Format(errTranslate.Error())
		errs = append(errs, errMar)
	}

	fCmd.Result = res
	fCmd.Error = cmderror.MostImportantCmdError(errs)
	return nil
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}
