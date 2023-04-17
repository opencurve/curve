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
* Created Date: 2023-04-18
* Author: chengyi01
 */
package pool

import (
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
)

// get pool total chunk size
type ChunkSizeCommand struct {
	basecmd.FinalCurveCmd
	metrics []*basecmd.Metric
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkSizeCommand)(nil) // check interface

func NewChunkSizeCommand() *cobra.Command {
	return NewQueryChunkSizeCommand().Cmd
}

func NewQueryChunkSizeCommand() *ChunkSizeCommand {
	queryCmd := &ChunkSizeCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&queryCmd.FinalCurveCmd, queryCmd)
	return queryCmd
}

func (cCmd *ChunkSizeCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
}

func (cCmd *ChunkSizeCommand) Init(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *ChunkSizeCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkSizeCommand) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (cCmd *ChunkSizeCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}
