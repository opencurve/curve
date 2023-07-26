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
 * Created Date: 2023-06-08
 * Author: CXF
 */

package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/space"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/chunkserver"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/etcd"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/mds"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/snapshot"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	TYPE_ETCD        = "etcd"
	TYPE_MDS         = "mds"
	TYPE_CHUNKSERVER = "chunkserver"
	TYPE_SNAPSHOT    = "snapshot"
	TYPE_COPYSET     = "copyset"
	TYPE_SPACE       = "space"
)

type ClusterCommand struct {
	basecmd.FinalCurveCmd
	tableRoles   *tablewriter.Table
	tableCopyset *tablewriter.Table
	tableSpace   *tablewriter.Table
	type2Func    map[string]func(caller *cobra.Command) (*interface{}, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS)
	health       cobrautil.ClUSTER_HEALTH_STATUS
}

var _ basecmd.FinalCurveCmdFunc = (*ClusterCommand)(nil) // check interface

const (
	clusterExample = `$ curve bs status cluster`
)

func NewClusterCommand() *cobra.Command {
	cCmd := &ClusterCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "cluster",
			Short:   "get status of the curvebs",
			Example: clusterExample,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd.Cmd
}

func (cCmd *ClusterCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBsMdsFlagOption(cCmd.Cmd)
}

func (cCmd *ClusterCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.tableRoles = tablewriter.NewWriter(os.Stdout)
	cCmd.tableCopyset = tablewriter.NewWriter(os.Stdout)
	cCmd.tableSpace = tablewriter.NewWriter(os.Stdout)

	rolesHeader := []string{cobrautil.ROW_ROLE, cobrautil.ROW_LEADER, cobrautil.ROW_VALUE_ONLINE, cobrautil.ROW_VALUE_OFFLINE}
	copysetHeader := []string{cobrautil.ROW_POOL_ID, cobrautil.ROW_TOTAL, cobrautil.COPYSET_OK_STR, cobrautil.COPYSET_WARN_STR, cobrautil.COPYSET_ERROR_STR}
	spaceHeader := []string{cobrautil.ROW_TYPE, cobrautil.ROW_USED, cobrautil.ROW_LEFT, cobrautil.ROW_RECYCLABLE, cobrautil.ROW_CREATED}
	cCmd.tableRoles.SetHeader(rolesHeader)
	cCmd.tableCopyset.SetHeader(copysetHeader)
	cCmd.tableSpace.SetHeader(spaceHeader)

	cCmd.tableRoles.SetRowLine(true)
	cCmd.tableRoles.SetAlignment(tablewriter.ALIGN_CENTER)

	cCmd.tableCopyset.SetRowLine(true)
	cCmd.tableCopyset.SetAlignment(tablewriter.ALIGN_CENTER)

	cCmd.tableSpace.SetRowLine(true)
	cCmd.tableSpace.SetAlignment(tablewriter.ALIGN_CENTER)

	cCmd.type2Func = map[string]func(caller *cobra.Command) (*interface{}, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS){
		TYPE_ETCD:        etcd.GetEtcdStatus,
		TYPE_MDS:         mds.GetMdsStatus,
		TYPE_CHUNKSERVER: chunkserver.GetChunkserverStatus,
		TYPE_SNAPSHOT:    snapshot.GetSnapshotStatus,
		TYPE_COPYSET:     copyset.GetCopysetsStatus,
		TYPE_SPACE:       space.GetSpaceStatus,
	}
	cCmd.health = cobrautil.HEALTH_OK
	return nil
}

func (cCmd *ClusterCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ClusterCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var errs []*cmderror.CmdError
	results := make(map[string]interface{})
	for key, function := range cCmd.type2Func {
		result, err, health := function(cmd)

		// snapshotserver addr is not configured
		if err != nil && err.Code == cmderror.ErrSnapShotAddrNotConfigured().Code && key == TYPE_SNAPSHOT {
			continue
		}

		errs = append(errs, err)
		cCmd.health = cobrautil.CompareHealth(cCmd.health, health)
		if result != nil && *result != nil {
			results[key] = *result
			// always true
			rows, ok := (*result).([]map[string]string)
			if !ok {
				retErr := cmderror.ErrConverResult()
				retErr.Format(key)
				return retErr.ToError()
			}
			cCmd.parseResults(rows, key)
		}
	}

	finalErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	cCmd.Error = finalErr
	results[cobrautil.HEALTH] = cobrautil.ClusterHealthStatus_Str[int32(cCmd.health)]
	cCmd.Result = results
	return nil
}

func (cCmd *ClusterCommand) ResultPlainOutput() error {
	fmt.Println("Services:")
	if cCmd.tableRoles != nil && cCmd.tableRoles.NumLines() > 0 {
		cCmd.tableRoles.Render()
	} else {
		fmt.Println("No found cluster services info")
	}

	fmt.Println("Copyset:")
	if cCmd.tableCopyset != nil && cCmd.tableCopyset.NumLines() > 0 {
		cCmd.tableCopyset.Render()
	} else {
		fmt.Println("No found copyset info")
	}

	fmt.Println("Space:")
	if cCmd.tableSpace != nil && cCmd.tableSpace.NumLines() > 0 {
		cCmd.tableSpace.Render()
	} else {
		fmt.Println("No found space info")
	}
	fmt.Println("Cluster health is:", cobrautil.ClusterHealthStatus_Str[int32(cCmd.health)])
	return nil
}

func (cCmd *ClusterCommand) parseResults(results []map[string]string, role string) {
	switch role {
	case TYPE_COPYSET:
		cCmd.buildTableCopyset(results)
	case TYPE_SPACE:
		cCmd.buildTableSpace(results)
	default:
		cCmd.buildTableRole(results, role)
	}
}

func (cCmd *ClusterCommand) buildTableRole(results []map[string]string, role string) {
	var leader string
	var onlineSlice, offlineSlice []string
	for _, row := range results {
		switch row[cobrautil.ROW_STATUS] {
		case cobrautil.ROW_LEADER:
			leader = row[cobrautil.ROW_ADDR]
		case cobrautil.ROW_VALUE_ONLINE, cobrautil.ROW_FOLLOWER:
			if role == TYPE_CHUNKSERVER {
				onlineSlice = append(onlineSlice, row[cobrautil.ROW_EXTERNAL_ADDR])
			} else {
				onlineSlice = append(onlineSlice, row[cobrautil.ROW_ADDR])
			}
		default:
			if role == TYPE_CHUNKSERVER {
				offlineSlice = append(offlineSlice, row[cobrautil.ROW_EXTERNAL_ADDR])
			} else {
				offlineSlice = append(offlineSlice, row[cobrautil.ROW_ADDR])
			}
		}
	}

	if role == TYPE_CHUNKSERVER {
		leader = "-"
	}

	online := strings.Join(onlineSlice, "\n")
	offline := strings.Join(offlineSlice, "\n")
	row := []string{role, leader, online, offline}
	cCmd.tableRoles.Append(row)
}

func (cCmd *ClusterCommand) buildTableSpace(results []map[string]string) {
	for _, row := range results {
		if row[cobrautil.ROW_TYPE] == cobrautil.ROW_VALUE_PHYSICAL {
			oneRow := []string{
				row[cobrautil.ROW_TYPE],
				row[cobrautil.ROW_USED],
				row[cobrautil.ROW_LEFT],
				row[cobrautil.ROW_RECYCLABLE],
				row[cobrautil.ROW_CREATED],
			}
			cCmd.tableSpace.Append(oneRow)
		}
	}
}

func (cCmd *ClusterCommand) buildTableCopyset(results []map[string]string) {
	var total, copysetOK, copysetWarn, copysetError int
	var PrevPoolID string
	var allRows [][]string = [][]string{}
	for _, row := range results {
		if PrevPoolID != row[cobrautil.ROW_POOL_ID] {
			if PrevPoolID != "" {
				allRows = append(
					allRows,
					[]string{
						PrevPoolID,
						strconv.Itoa(total),
						strconv.Itoa(copysetOK),
						strconv.Itoa(copysetWarn),
						strconv.Itoa(copysetError),
					})
			}
			PrevPoolID = row[cobrautil.ROW_POOL_ID]
			total, copysetOK, copysetWarn, copysetError = 0, 0, 0, 0
		}

		switch row[cobrautil.ROW_STATUS] {
		case cobrautil.COPYSET_OK_STR:
			copysetOK++
		case cobrautil.COPYSET_WARN_STR:
			copysetWarn++
		case cobrautil.COPYSET_ERROR_STR:
			copysetError++
		}

		total++
	}

	if PrevPoolID != "" {
		allRows = append(
			allRows,
			[]string{
				PrevPoolID,
				strconv.Itoa(total),
				strconv.Itoa(copysetOK),
				strconv.Itoa(copysetWarn),
				strconv.Itoa(copysetError),
			})
	}
	cCmd.tableCopyset.AppendBulk(allRows)
}
