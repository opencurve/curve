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
* Created Date: 2023-12-11
* Author: wsehjk
 */

package target

import (
	"encoding/json"
	"fmt"
	"os"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/client"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/status/cluster"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	targetExample = "$curve bs export target"
)
type ResultStruct map[string]interface{}
type targetcommand struct {
	basecmd.FinalCurveCmd
}
// check interface targetcommand implement FinalCurveCmdFunc
var _ basecmd.FinalCurveCmdFunc = (*targetcommand)(nil)  

func ExtractClusterResult(clusterResult map[string]interface{}, service string) ResultStruct{
	result := ResultStruct{}
	arrs := clusterResult[service].([]map[string]string)
	result[cobrautil.LabelKey] = map[string]string{
		cobrautil.JobKey : service,
	}
	if service == cobrautil.SnapshotTarget {
		result[cobrautil.LabelKey] = map[string]string{
			cobrautil.JobKey  : cobrautil.SnapshotServerTarget,
		}	
	}
	result[cobrautil.TargetKey] = make([]string, 0)
	for _, arr := range arrs {
		if addr, ok := arr["addr"]; ok {  // see the output of curve bs status cluster
			result[cobrautil.TargetKey] = append(result[cobrautil.TargetKey].([]string), addr)
		}
		if addr, ok := arr["internalAddr"]; ok {
			result[cobrautil.TargetKey] = append(result[cobrautil.TargetKey].([]string), addr)
		}
	}
	return result
}
func NewTargetCommand() (cmd *cobra.Command) {
	tCmd := &targetcommand{
		FinalCurveCmd: basecmd.FinalCurveCmd {
			Use: "target",
			Short: "export all the target in cluster and used by monitor",
			Example: targetExample,
		},
	}
	return basecmd.NewFinalCurveCli(&tCmd.FinalCurveCmd, tCmd)
}

func (tCmd *targetcommand)Init(cmd *cobra.Command, args []string) error {
	return nil
}
func (tCmd *targetcommand)RunCommand(cmd *cobra.Command, args []string) error {
	// get cluster target 
	clusterResult, err := cluster.GetClusterStatus()
	if err != nil {
		retErr := cmderror.ErrTargetCluster()
		retErr.Format(err.Error())
		return retErr.ToError()
	}
	tCmd.Result = []ResultStruct{}
	tCmd.Result = append(tCmd.Result.([]ResultStruct), ExtractClusterResult(clusterResult.(map[string]interface{}), cobrautil.EtcdTarget))
	tCmd.Result = append(tCmd.Result.([]ResultStruct), ExtractClusterResult(clusterResult.(map[string]interface{}), cobrautil.MdsTarget))
	tCmd.Result = append(tCmd.Result.([]ResultStruct), ExtractClusterResult(clusterResult.(map[string]interface{}), cobrautil.ChunkServerTarget))
	tCmd.Result = append(tCmd.Result.([]ResultStruct), ExtractClusterResult(clusterResult.(map[string]interface{}), cobrautil.SnapshotTarget))
	
	// get client target 
	clientResult, err := client.GetClientStatus()
	if err != nil {
		retErr := cmderror.ErrTargetClient()
		retErr.Format(err.Error())
		return retErr.ToError()
	}
	clientStruct := ResultStruct{}
	clientStruct[cobrautil.LabelKey] = map[string]string{
		cobrautil.JobKey : cobrautil.ClientTarget,
	}
	clientStruct[cobrautil.TargetKey] = make([]string, 0)
	for _, result := range clientResult.([]map[string]string) {
		clientStruct[cobrautil.TargetKey] = append(clientStruct[cobrautil.TargetKey].([]string), result["addr"])	
	}
	tCmd.Result = append(tCmd.Result.([]ResultStruct), clientStruct)
	return nil
}

func (tCmd *targetcommand)Print(cmd *cobra.Command, args []string) error {
	output, err := json.MarshalIndent(tCmd.Result, "", "  ")
	if err != nil {
		return err
	}
	path := tCmd.Cmd.Flag(cobrautil.TargetPathFlag).Value.String()
	if err = os.WriteFile(path + "/" + cobrautil.OutputFile, output, 0666); err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}
// result in plain format string
func (tCmd *targetcommand)ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd)
}

// target final command has no flag 
func (tCmd *targetcommand)AddFlags() { 
	tCmd.Cmd.Flags().String(cobrautil.TargetPathFlag, ".", "specify the path of target.json")
}