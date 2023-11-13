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

package mds

import (
	"context"
	"fmt"

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
	example = `$ curve fs update mds client-mds-addrs-override 1.2.3.4:1234,2.3.4.5:2345`
)

type SetClientMdsAddrsOverride struct {
	Info      *basecmd.Rpc
	Request   *mds.SetClientMdsAddrsOverrideRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*SetClientMdsAddrsOverride)(nil) // check interface

type SetClientMdsAddrsOverrideCommand struct {
	basecmd.FinalCurveCmd
	Rpc *SetClientMdsAddrsOverride
}

var _ basecmd.FinalCurveCmdFunc = (*SetClientMdsAddrsOverrideCommand)(nil) // check interface

func (ufRp *SetClientMdsAddrsOverride) NewRpcClient(cc grpc.ClientConnInterface) {
	ufRp.mdsClient = mds.NewMdsServiceClient(cc)
}

func (ufRp *SetClientMdsAddrsOverride) Stub_Func(ctx context.Context) (interface{}, error) {
	return ufRp.mdsClient.SetClientMdsAddrsOverride(ctx, ufRp.Request)
}

func NewSetClientMdsAddrsOverrideCommand() *cobra.Command {
	cmd := &SetClientMdsAddrsOverrideCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "client-mds-addrs-override",
			Short:   "set client mds addrs override",
			Long:    "set client mds addrs override",
			Example: example,
		},
	}
	basecmd.NewFinalCurveCli(&cmd.FinalCurveCmd, cmd)
	return cmd.Cmd
}

func (fCmd *SetClientMdsAddrsOverrideCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddFsMdsAddrFlag(fCmd.Cmd)
}

func (fCmd *SetClientMdsAddrsOverrideCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(fCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)

	// output format
	header := []string{cobrautil.ROW_RESULT}
	fCmd.SetHeader(header)

	if len(args) != 1 {
		return fmt.Errorf("please specify client mds addrs override, example: " + example)
	}

	request := &mds.SetClientMdsAddrsOverrideRequest{
		ClientMdsAddrsOverride: &args[0],
	}
	// set rpc
	fCmd.Rpc = &SetClientMdsAddrsOverride{
		Request: request,
	}
	fCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "change client mds addrs override")
	return nil
}

func (fCmd *SetClientMdsAddrsOverrideCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *SetClientMdsAddrsOverrideCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, errCmd := basecmd.GetRpcResponse(fCmd.Rpc.Info, fCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}

	response := result.(*mds.SetClientMdsAddrsOverrideResponse)
	errMsg := mds.FSStatusCode_name[int32(response.GetStatusCode())]
	row := map[string]string{
		cobrautil.ROW_RESULT: errMsg,
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

func (fCmd *SetClientMdsAddrsOverrideCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}
