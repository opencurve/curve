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
 * Created Date: 2023-12-07
 * Author: Cao Xianfei (caoxianfei1)
 */

package space

import (
	"context"
	"fmt"
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	spaceExample = `$ curve fs list space --fs <fsname>`
)

const (
	urlPrefix = "/vars/"

	KEY_USED      = "used"
	KEY_INODE_NUM = "inode_num"

	unit            = 1024
	MiB             = 1048576
	GiB             = 1073741824
	DefaultCapacity = "10PB"
)

var (
	metrics = map[string]string{
		"used":      "fs_usage_info_fs_" + "%s" + "_used",    // used
		"inode_num": "topology_fs_id_" + "%d" + "_inode_num", // inode_num
	}
)

type ListSpaceRpc struct {
	Info      *basecmd.Rpc
	Request   *mds.GetFsInfoRequest
	mdsClient mds.MdsServiceClient
}

var _ basecmd.RpcFunc = (*ListSpaceRpc)(nil) // check interface

func (sRpc *ListSpaceRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	sRpc.mdsClient = mds.NewMdsServiceClient(cc)
}

func (sRpc *ListSpaceRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return sRpc.mdsClient.GetFsInfo(ctx, sRpc.Request)
}

var _ basecmd.FinalCurveCmdFunc = (*SpaceCommand)(nil) // check interface

type SpaceCommand struct {
	basecmd.FinalCurveCmd
	Rpc      *ListSpaceRpc
	metrics  map[string]*basecmd.Metric
	response *mds.GetFsInfoResponse

	addrs  []string
	fsName string
}

func NewSpaceCommand() *cobra.Command {
	return NewListSpaceCommand().Cmd
}

func NewListSpaceCommand() *SpaceCommand {
	spaceCmd := &SpaceCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "space",
			Short:   "list the space of specified fs",
			Example: spaceExample,
		},
	}
	basecmd.NewFinalCurveCli(&spaceCmd.FinalCurveCmd, spaceCmd)
	return spaceCmd
}

func (sCmd *SpaceCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddFsMdsAddrFlag(sCmd.Cmd)
	config.AddFsRequiredFlag(sCmd.Cmd)
}

func (sCmd *SpaceCommand) Init(cmd *cobra.Command, args []string) error {
	// build rpc
	sCmd.Rpc = &ListSpaceRpc{}
	addrs, addrErr := config.GetFsMdsAddrSlice(sCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	sCmd.metrics = make(map[string]*basecmd.Metric)
	sCmd.addrs = []string{}
	sCmd.addrs = addrs
	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	fsName, err := cmd.Flags().GetString(config.CURVEFS)
	if err != nil {
		return fmt.Errorf("get flag %s failed from cmd: %v", config.CURVEFS, err)
	}
	sCmd.fsName = fsName
	sCmd.Rpc.Request = &mds.GetFsInfoRequest{FsName: &fsName}
	sCmd.Rpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetFsInfo")

	// build table header
	header := []string{
		cobrautil.ROW_FS_NAME,
		cobrautil.ROW_CAPACITY,
		cobrautil.ROW_USED,
		cobrautil.ROW_INODE_NUM,
	}
	sCmd.SetHeader(header)

	return nil
}

func (sCmd *SpaceCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SpaceCommand) RunCommand(cmd *cobra.Command, args []string) error {
	// rpc response
	response, errCmd := basecmd.GetRpcResponse(sCmd.Rpc.Info, sCmd.Rpc)
	if errCmd.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(errCmd.Message)
	}
	sCmd.response = response.(*mds.GetFsInfoResponse)
	capacity := sCmd.response.GetFsInfo().GetCapacity()
	fsId := sCmd.response.GetFsInfo().GetFsId()
	fsName := sCmd.response.GetFsInfo().GetFsName()

	sCmd.Error = cmderror.ErrSuccess()

	rows := make([]map[string]string, 0)
	row := make(map[string]string)
	row[cobrautil.ROW_FS_NAME] = fsName
	row[cobrautil.ROW_CAPACITY] = strconv.FormatUint(capacity, 10)
	capStr := convertStr(capacity)
	row[cobrautil.ROW_CAPACITY] = capStr
	if row[cobrautil.ROW_CAPACITY] == "0" {
		row[cobrautil.ROW_CAPACITY] = DefaultCapacity
	}

	// metric response
	for key, subUri := range metrics {
		if key == KEY_USED {
			subUri = fmt.Sprintf(subUri, sCmd.fsName)
		} else if key == KEY_INODE_NUM {
			subUri = fmt.Sprintf(subUri, fsId)
		}
		subUri = urlPrefix + subUri
		m := basecmd.NewMetric(sCmd.addrs, subUri, viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT))
		sCmd.metrics[key] = m
	}

	for key, metric := range sCmd.metrics {
		kvValue, err := basecmd.QueryMetric(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		used, err := basecmd.GetMetricValue(kvValue)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}

		if key == KEY_USED {
			row[cobrautil.ROW_USED] = used
			value, _ := strconv.ParseUint(used, 10, 64)
			valueStr := convertStr(value)
			row[cobrautil.ROW_USED] = valueStr
		} else if key == KEY_INODE_NUM {
			row[cobrautil.ROW_INODE_NUM] = used
		}
	}
	rows = append(rows, row)

	if len(rows) > 0 {
		list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{
			config.CURVEFS_FSID,
		})
		sCmd.TableNew.AppendBulk(list)
		sCmd.Result = rows
	}

	return nil
}

func (sCmd *SpaceCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}

func convertStr(capacity uint64) string {
	var transRes float64
	var transResStr string
	if capacity < MiB {
		transResStr = strconv.FormatUint(capacity, 10)
	} else if capacity >= MiB && capacity < GiB {
		transRes, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(capacity)/float64(unit)/float64(unit)), 64)
		transResStr = strconv.FormatFloat(transRes, 'f', -1, 64) + "MiB"
	} else if capacity >= GiB {
		transRes, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(capacity)/float64(unit)/float64(unit)/float64(unit)), 64)
		transResStr = strconv.FormatFloat(transRes, 'f', -1, 64) + "GiB"
	}
	return transResStr
}
