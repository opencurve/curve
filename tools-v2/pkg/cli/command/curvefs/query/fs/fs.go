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
 * Created Date: 2022-06-15
 * Author: chengyi (Cyber-SiKu)
 */

package fs

import (
	"context"
	"fmt"
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	mds "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	fsExample = `$ curve fs query fs --fsid 1,2,3
$ curve fs query fs --fsname test1,test2,test3`
)

type QueryFsRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.GetFsInfoRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*QueryFsRpc)(nil) // check interface

type FsCommand struct {
	basecmd.FinalCurveCmd
	Rpc  []*QueryFsRpc
	Rows []map[string]string
}

var _ basecmd.FinalCurveCmdFunc = (*FsCommand)(nil) // check interface

func (qfRp *QueryFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	qfRp.mdsClient = mds.NewMdsServiceClient(cc)
}

func (qfRp *QueryFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return qfRp.mdsClient.GetFsInfo(ctx, qfRp.Request)
}

func NewFsCommand() *cobra.Command {
	fsCmd := &FsCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "fs",
			Short:   "query fs in curvefs by fsname or fsid",
			Long:    "when both fsname and fsid exist, query only by fsid",
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
	config.AddFsNameSliceOptionFlag(fCmd.Cmd)
	config.AddFsIdSliceOptionFlag(fCmd.Cmd)
}

func (fCmd *FsCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(fCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	var fsIds []string
	var fsNames []string
	if !fCmd.Cmd.Flag(config.CURVEFS_FSNAME).Changed && !fCmd.Cmd.Flag(config.CURVEFS_FSID).Changed {
		return fmt.Errorf("fsname or fsid is required")
	}
	if fCmd.Cmd.Flag(config.CURVEFS_FSNAME).Changed && !fCmd.Cmd.Flag(config.CURVEFS_FSID).Changed {
		// fsname is set, but fsid is not set
		fsNames, _ = fCmd.Cmd.Flags().GetStringSlice(config.CURVEFS_FSNAME)
	} else {
		fsIds = viper.GetStringSlice(config.VIPER_CURVEFS_FSID)
	}

	if len(fsIds) == 0 && len(fsNames) == 0 {
		return fmt.Errorf("fsname or fsid is required")
	}

	header := []string{curveutil.ROW_ID, curveutil.ROW_NAME, curveutil.ROW_STATUS, curveutil.ROW_CAPACITY, curveutil.ROW_BLOCKSIZE, curveutil.ROW_FS_TYPE, curveutil.ROW_SUM_IN_DIR, curveutil.ROW_OWNER, curveutil.ROW_MOUNT_NUM}
	fCmd.SetHeader(header)
	fCmd.TableNew.SetAutoMergeCellsByColumnIndex(
		curveutil.GetIndexSlice(header, []string{curveutil.ROW_FS_TYPE}),
	)

	fCmd.Rows = make([]map[string]string, 0)
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	for i := range fsNames {
		request := &mds.GetFsInfoRequest{
			FsName: &fsNames[i],
		}
		rpc := &QueryFsRpc{
			Request: request,
		}
		rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetFsInfo")
		fCmd.Rpc = append(fCmd.Rpc, rpc)
		row := make(map[string]string)
		row[curveutil.ROW_NAME] = fsNames[i]
		row[curveutil.ROW_ID] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_STATUS] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_CAPACITY] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_BLOCKSIZE] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_FS_TYPE] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_SUM_IN_DIR] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_OWNER] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_MOUNT_NUM] = curveutil.ROW_VALUE_DNE
		fCmd.Rows = append(fCmd.Rows, row)
	}

	for i := range fsIds {
		id, err := strconv.ParseUint(fsIds[i], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid fsId: %s", fsIds[i])
		}
		id32 := uint32(id)
		request := &mds.GetFsInfoRequest{
			FsId: &id32,
		}
		rpc := &QueryFsRpc{
			Request: request,
		}
		rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetFsInfo")
		fCmd.Rpc = append(fCmd.Rpc, rpc)
		row := make(map[string]string)
		row[curveutil.ROW_ID] = fsIds[i]
		row[curveutil.ROW_NAME] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_STATUS] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_CAPACITY] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_BLOCKSIZE] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_FS_TYPE] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_SUM_IN_DIR] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_OWNER] = curveutil.ROW_VALUE_DNE
		row[curveutil.ROW_MOUNT_NUM] = curveutil.ROW_VALUE_DNE
		fCmd.Rows = append(fCmd.Rows, row)
	}

	return nil
}

func (fCmd *FsCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func (fCmd *FsCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var infos []*basecmd.Rpc
	var funcs []basecmd.RpcFunc
	for _, rpc := range fCmd.Rpc {
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
		if result == nil {
			continue
		}
		response := result.(*mds.GetFsInfoResponse)
		res, err := output.MarshalProtoJson(response)
		if err != nil {
			errMar := cmderror.ErrMarShalProtoJson()
			errMar.Format(err.Error())
			errs = append(errs, errMar)
		}
		resList = append(resList, res)
		if response.GetStatusCode() != mds.FSStatusCode_OK {
			code := response.GetStatusCode()
			err := cmderror.ErrGetFsInfo(int(code))
			err.Format(mds.FSStatusCode_name[int32(response.GetStatusCode())])
			errs = append(errs, err)
			continue
		}
		fsInfo := response.GetFsInfo()
		for _, row := range fCmd.Rows {
			id := strconv.FormatUint(uint64(fsInfo.GetFsId()), 10)
			if row[curveutil.ROW_ID] == id || row[curveutil.ROW_NAME] == fsInfo.GetFsName() {
				row[curveutil.ROW_ID] = id
				row[curveutil.ROW_NAME] = fsInfo.GetFsName()
				row[curveutil.ROW_STATUS] = fsInfo.GetStatus().String()
				row[curveutil.ROW_CAPACITY] = fmt.Sprintf("%d", fsInfo.GetCapacity())
				row[curveutil.ROW_BLOCKSIZE] = fmt.Sprintf("%d", fsInfo.GetBlockSize())
				row[curveutil.ROW_FS_TYPE] = fsInfo.GetFsType().String()
				row[curveutil.ROW_SUM_IN_DIR] = fmt.Sprintf("%t", fsInfo.GetEnableSumInDir())
				row[curveutil.ROW_OWNER] = fsInfo.GetOwner()
				row[curveutil.ROW_MOUNT_NUM] = fmt.Sprintf("%d", fsInfo.GetMountNum())
			}
		}
	}

	list := curveutil.ListMap2ListSortByKeys(fCmd.Rows, fCmd.Header, []string{
		curveutil.ROW_FS_TYPE, curveutil.ROW_ID,
	})
	fCmd.TableNew.AppendBulk(list)
	fCmd.Result = resList
	fCmd.Error = cmderror.MostImportantCmdError(errs)

	return nil
}

func (fCmd *FsCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}
