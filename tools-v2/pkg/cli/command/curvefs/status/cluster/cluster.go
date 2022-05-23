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
 * Created Date: 2022-06-25
 * Author: chengyi (Cyber-SiKu)
 */

package cluster

import (
	"fmt"

	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/status/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/status/etcd"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/status/mds"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/status/metaserver"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	TYPE_ETCD        = "etcd"
	TYPE_MDS         = "mds"
	TYPE_MEATASERVER = "meataserver"
	TYPE_COPYSET     = "copyset"
)

type ClusterCommand struct {
	basecmd.FinalCurveCmd
	type2Table map[string]*table.Table
	type2Func   map[string]func(caller *cobra.Command) (*interface{}, *table.Table, *cmderror.CmdError)
	serverList []string
}

var _ basecmd.FinalCurveCmdFunc = (*ClusterCommand)(nil) // check interface

func NewClusterCommand() *cobra.Command {
	cCmd := &ClusterCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:   "cluster",
			Short: "get status of the curvefs",
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd.Cmd
}

func (cCmd *ClusterCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
}

func (cCmd *ClusterCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.type2Func = map[string]func(caller *cobra.Command) (*interface{}, *table.Table, *cmderror.CmdError){
		TYPE_ETCD:        etcd.GetEtcdStatus,
		TYPE_MDS:         mds.GetMdsStatus,
		TYPE_MEATASERVER: metaserver.GetMetaserverStatus,
		TYPE_COPYSET:     copyset.GetCopysetStatus,
	}
	cCmd.type2Table = make(map[string]*table.Table)
	cCmd.serverList = []string{TYPE_ETCD, TYPE_MDS, TYPE_MEATASERVER, TYPE_COPYSET}
	return nil
}

func (cCmd *ClusterCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ClusterCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var errs []*cmderror.CmdError
	results := make(map[string]interface{})
	for key, function := range cCmd.type2Func {
		result, table, err := function(cmd)
		cCmd.type2Table[key] = table
		results[key] = *result
		errs = append(errs, err)
	}
	finalErr := cmderror.MergeCmdError(errs)
	cCmd.Error = &finalErr
	cCmd.Result = results
	return nil
}

func (cCmd *ClusterCommand) ResultPlainOutput() error {
	for _, server := range cCmd.serverList {
		fmt.Printf("%s:\n", server)
		fmt.Println(cCmd.type2Table[server])
	}
	return nil
}
