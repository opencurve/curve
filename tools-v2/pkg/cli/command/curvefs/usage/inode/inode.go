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
 * Created Date: 2022-05-25
 * Author: chengyi (Cyber-SiKu)
 */

package inode

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	PREFIX = "topology_fs_id"
	SUFFIX = "inode_num"
)

type InodeNumCommand struct {
	basecmd.FinalCurveCmd
	FsId2Filetype2Metric map[string]map[string]basecmd.Metric
}

type Result struct {
	Result string
	Error  *cmderror.CmdError
	SubUri string
}

var _ basecmd.FinalCurveCmdFunc = (*InodeNumCommand)(nil) // check interface

func NewInodeNumCommand() *cobra.Command {
	inodeNumCmd := &InodeNumCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "inode",
			Short: "get the inode usage of curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&inodeNumCmd.FinalCurveCmd, inodeNumCmd)
	return inodeNumCmd.Cmd
}

func (iCmd *InodeNumCommand) AddFlags() {
	config.AddFsMdsAddrFlag(iCmd.Cmd)
	config.AddFsIdOptionDefaultAllFlag(iCmd.Cmd)
	config.AddHttpTimeoutFlag(iCmd.Cmd)
}

func (iCmd *InodeNumCommand) Init(cmd *cobra.Command, args []string) error {
	addrs, addrErr := config.GetFsMdsAddrSlice(iCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}

	iCmd.FsId2Filetype2Metric = make(map[string]map[string]basecmd.Metric)

	fsIds := viper.GetStringSlice(config.VIPER_CURVEFS_FSID)
	if len(fsIds) == 0 {
		fsIds = []string{"*"}
	}
	for _, fsId := range fsIds {
		_, err := strconv.ParseUint(fsId, 10, 32)
		if err != nil && fsId != "*" {
			return fmt.Errorf("invalid fsId: %s", fsId)
		}
		subUri := fmt.Sprintf("/vars/"+PREFIX+"_%s*"+SUFFIX, fsId)
		timeout := viper.GetDuration(config.VIPER_GLOBALE_HTTPTIMEOUT)
		metric := *basecmd.NewMetric(addrs, subUri, timeout)
		filetype2Metric := make(map[string]basecmd.Metric)
		filetype2Metric["inode_num"] = metric
		iCmd.FsId2Filetype2Metric[fsId] = filetype2Metric
	}
	table, err := gotable.Create("fsId", "filetype", "num")
	if err != nil {
		cobra.CheckErr(err)
	}
	iCmd.Table = table
	return nil
}

func (iCmd *InodeNumCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&iCmd.FinalCurveCmd, iCmd)
}

func (iCmd *InodeNumCommand) RunCommand(cmd *cobra.Command, args []string) error {
	results := make(chan Result, config.MaxChannelSize())
	size := 0
	for fsId, filetype2Metric := range iCmd.FsId2Filetype2Metric {
		for filetype, metric := range filetype2Metric {
			size++
			go func(m basecmd.Metric, filetype string, id string) {
				result, err := basecmd.QueryMetric(m)
				results <- Result{
					Result: result,
					Error:  err,
					SubUri: m.SubUri,
				}
			}(metric, filetype, fsId)
		}
	}
	count := 0
	rows := make([]map[string]string, 0)
	var errs []*cmderror.CmdError
	for res := range results {
		datas := strings.Split(res.Result, "\n")
		if res.Error.Code == cmderror.CODE_SUCCESS {
			for _, data := range datas {
				if data == "" {
					continue
				}
				data = cobrautil.RmWitespaceStr(data)
				resMap := strings.Split(data, ":")
				preMap := strings.Split(resMap[0], "_")
				if len(resMap) != 2 && len(preMap) < 4 {
					splitErr := cmderror.ErrDataNoExpected()
					splitErr.Format(data, "the length of the data does not meet the requirements")
					errs = append(errs, splitErr)
				} else {
					num, errNum := strconv.ParseInt(resMap[1], 10, 64)
					id, errId := strconv.ParseUint(preMap[3], 10, 32)
					prefix := fmt.Sprintf("%s_%s_", PREFIX, preMap[3])
					filetype := strings.Replace(resMap[0], prefix, "", 1)
					filetype = strings.Replace(filetype, SUFFIX, "", 1)
					if filetype == "" {
						filetype = "inode_num_"
					}
					filetype = filetype[0 : len(filetype)-1]
					if errNum == nil && errId == nil {
						row := make(map[string]string)
						row["fsId"] = strconv.FormatUint(uint64(id), 10)
						row["filetype"] = filetype
						row["num"] = strconv.FormatInt(num, 10)
						rows = append(rows, row)
					} else {
						toErr := cmderror.ErrDataNoExpected()
						toErr.Format(data)
						errs = append(errs, toErr)
					}
				}
			}
		}
		count++
		if count >= size {
			break
		}
	}
	iCmd.Error = cmderror.MostImportantCmdError(errs)

	if len(rows) > 0 {
		// query some data
		iCmd.Table.AddRows(rows)
		jsonResult, err := iCmd.Table.JSON(0)
		if err != nil {
			cobra.CheckErr(err)
		}
		var m interface{}
		err = json.Unmarshal([]byte(jsonResult), &m)
		if err != nil {
			cobra.CheckErr(err)
		}
		iCmd.Result = m
	}
	return nil
}

func (iCmd *InodeNumCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&iCmd.FinalCurveCmd, iCmd)
}
