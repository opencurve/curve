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
/*
* Project: curve
* Created Date: 2023-08-01
* Author: xuchaojie
 */
package flatten

import (
	"context"
	"time"
    "fmt"
    "os"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
    "github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	flattenExample = `$ curve bs flatten --path /test2 --user curve`
)

type FlattenRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.FlattenRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*FlattenRpc)(nil) // check interface


func (eRpc *FlattenRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *FlattenRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return eRpc.mdsClient.Flatten(ctx, eRpc.Request)
}

type QueryFlattenStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.QueryFlattenStatusRequest
	mdsClient nameserver2.CurveFSServiceClient
}

var _ basecmd.RpcFunc = (*QueryFlattenStatusRpc)(nil) // check interface


func (eRpc *QueryFlattenStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	eRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (eRpc *QueryFlattenStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return eRpc.mdsClient.QueryFlattenStatus(ctx, eRpc.Request)
}

type flattenCommand struct {
	basecmd.FinalCurveCmd
	flattenRpc      *FlattenRpc
	flattenResponse *nameserver2.FlattenResponse
    queryFlattenStatusRpc  *QueryFlattenStatusRpc
    queryFlattenStatusResponse *nameserver2.QueryFlattenStatusResponse
}

var _ basecmd.FinalCurveCmdFunc = (*flattenCommand)(nil)

func (fCmd *flattenCommand) Init(cmd *cobra.Command, args []string) error {
	fileName := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PATH)
	date, errDat := cobrautil.GetTimeofDayUs()
	owner := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return errDat.ToError()
	}
    flattenRequest := &nameserver2.FlattenRequest {
          FileName: &fileName,
		  Date:     &date,
		  Owner:    &owner,
    }
    queryFlattenStatusRequest := &nameserver2.QueryFlattenStatusRequest {
          FileName: &fileName,
		  Date:     &date,
		  Owner:    &owner,
    }

	password := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PASSWORD)
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		flattenRequest.Signature = &sig
        queryFlattenStatusRequest.Signature = &sig
	}
	mdsAddrs, errMds := config.GetBsMdsAddrSlice(fCmd.Cmd)
	if errMds.TypeCode() != cmderror.CODE_SUCCESS {
		return errMds.ToError()
	}
	timeout := config.GetFlagDuration(fCmd.Cmd, config.RPCTIMEOUT)
	retrytimes := config.GetFlagInt32(fCmd.Cmd, config.RPCRETRYTIMES)
    fCmd.flattenRpc = &FlattenRpc{
        Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "flatten"),
            Request: flattenRequest,
    }
    fCmd.queryFlattenStatusRpc = &QueryFlattenStatusRpc{
        Info: basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "query flatten status"),
            Request: queryFlattenStatusRequest,
    }
	return nil
}

func (fCmd *flattenCommand) RunCommand(cmd *cobra.Command, args []string) error {
	result, err := basecmd.GetRpcResponse(fCmd.flattenRpc.Info, fCmd.flattenRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	fCmd.flattenResponse = result.(*nameserver2.FlattenResponse)
	fCmd.Result = fCmd.flattenResponse

    fileName:= fCmd.flattenRpc.Request.GetFileName() 

	if fCmd.flattenResponse.GetStatusCode() != nameserver2.StatusCode_kOK {
		err = cmderror.ErrBsFlatten(fCmd.flattenResponse.GetStatusCode(), fileName)
		return err.ToError()
	}

    bar := progressbar.NewOptions(100,
        progressbar.OptionSetDescription("[cyan]Flattening[reset] "+fileName+"..."),
        progressbar.OptionShowCount(),
        progressbar.OptionShowIts(),
        progressbar.OptionSpinnerType(14),
        progressbar.OptionFullWidth(),
        progressbar.OptionSetRenderBlankState(true),
        progressbar.OptionOnCompletion(func() {
            fmt.Fprint(os.Stderr, "\n")
        }),
        progressbar.OptionEnableColorCodes(true),
        progressbar.OptionSetTheme(progressbar.Theme{
            Saucer:        "[green]=[reset]",
            SaucerHead:    "[green]>[reset]",
            SaucerPadding: " ",
            BarStart:      "[",
            BarEnd:        "]",
    }))
    for {
        time.Sleep(100 * time.Millisecond)
        // update Signature & date
	    date, errDat := cobrautil.GetTimeofDayUs()
        if errDat.TypeCode() != cmderror.CODE_SUCCESS {
            return errDat.ToError()
        }
	    owner := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_USER)
	    password := config.GetBsFlagString(fCmd.Cmd, config.CURVEBS_PASSWORD)
        if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
            strSig := cobrautil.GetString2Signature(date, owner)
            sig := cobrautil.CalcString2Signature(strSig, password)
            fCmd.queryFlattenStatusRpc.Request.Signature = &sig
            fCmd.queryFlattenStatusRpc.Request.Date = &date
        }

        result, err := basecmd.GetRpcResponse(fCmd.queryFlattenStatusRpc.Info, fCmd.queryFlattenStatusRpc)
        if err.TypeCode() != cmderror.CODE_SUCCESS {
            return err.ToError()
        }
        fCmd.queryFlattenStatusResponse = result.(*nameserver2.QueryFlattenStatusResponse)
        fCmd.Result = fCmd.queryFlattenStatusResponse
        if fCmd.queryFlattenStatusResponse.GetStatusCode() != nameserver2.StatusCode_kOK {
            err = cmderror.ErrBsFlatten(fCmd.queryFlattenStatusResponse.GetStatusCode(), fileName)
            return err.ToError()
        }
        process := result.(*nameserver2.QueryFlattenStatusResponse).GetProgress()
        bar.Set(int(process))
        FileStatus := result.(*nameserver2.QueryFlattenStatusResponse).GetFileStatus()
        if (FileStatus == nameserver2.FileStatus_kFileCreated) {
            bar.Set(100)
            break
        }
    }
    bar.Finish()
	return nil
}

func (fCmd *flattenCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *flattenCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}

func (fCmd *flattenCommand) AddFlags() {
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddBsPathRequiredFlag(fCmd.Cmd)
	config.AddBsUserRequiredFlag(fCmd.Cmd)
	config.AddBsPasswordOptionFlag(fCmd.Cmd)
}

func NewFlattenCommand() *cobra.Command {
	fCmd := &flattenCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "flatten",
			Short:   "flatten clone file in curvebs cluster",
			Example: flattenExample,
		},
	}
	return basecmd.NewFinalCurveCli(&fCmd.FinalCurveCmd, fCmd)
}

