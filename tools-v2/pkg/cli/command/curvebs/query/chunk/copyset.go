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
* Created Date: 2023-04-12
* Author: chengyi01
 */

package chunk

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/seginfo"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
)

type ChunkCopysetCommand struct {
	basecmd.FinalCurveCmd
	FileInfo      *nameserver2.FileInfo
	ChunkId       uint64
	LogicalpoolId uint32
	CopysetId     uint32
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkCopysetCommand)(nil) // check interface

func NewQueryChunkCopysetCommand() *ChunkCopysetCommand {
	chunkCmd := &ChunkCopysetCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{},
	}

	basecmd.NewFinalCurveCli(&chunkCmd.FinalCurveCmd, chunkCmd)
	return chunkCmd
}

func NewChunkCopysetCommand() *cobra.Command {
	return NewQueryChunkCopysetCommand().Cmd
}

func (cCmd *ChunkCopysetCommand) AddFlags() {
	config.AddBsMdsFlagOption(cCmd.Cmd)
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddBsPathRequiredFlag(cCmd.Cmd)
	config.AddBsOffsetRequiredFlag(cCmd.Cmd)
	config.AddBsUserOptionFlag(cCmd.Cmd)
	config.AddBsPasswordOptionFlag(cCmd.Cmd)
}

func (cCmd *ChunkCopysetCommand) Init(cmd *cobra.Command, args []string) error {
	fileInfoResponse, err := file.GetFileInfo(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	cCmd.FileInfo = fileInfoResponse.GetFileInfo()
	if cCmd.FileInfo.GetFileType() != nameserver2.FileType_INODE_PAGEFILE {
		filepath := config.GetBsFlagString(cCmd.Cmd, config.CURVEBS_PATH)
		return fmt.Errorf("file %s is not a pagefile", filepath)
	}
	return nil
}

func (cCmd *ChunkCopysetCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ChunkCopysetCommand) RunCommand(cmd *cobra.Command, args []string) error {
	segmentSize := uint64(cCmd.FileInfo.GetSegmentSize())
	offset := config.GetBsFlagUint64(cCmd.Cmd, config.CURVEBS_OFFSET)
	segOffset := (offset / segmentSize) * segmentSize

	cCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEBS_OFFSET), fmt.Sprintf("%d", segOffset),
	})
	segmentRes, err := seginfo.GetSegment(cCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	segment := segmentRes.GetPageFileSegment()
	chunkSize := segment.GetChunkSize()
	if chunkSize == 0 {
		return fmt.Errorf("no chunks in segment")
	}
	chunkIndex := (offset - segOffset) / uint64(chunkSize)
	chunks := segment.GetChunks()
	if chunkIndex >= uint64(len(chunks)) {
		return fmt.Errorf("chunkIndex exceed chunks num in segment")
	}
	chunk := chunks[chunkIndex]
	cCmd.ChunkId = chunk.GetChunkID()
	cCmd.LogicalpoolId = segment.GetLogicalPoolID()
	cCmd.CopysetId = chunk.GetCopysetID()
	return nil
}

func (cCmd *ChunkCopysetCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cCmd.FinalCurveCmd)
}

// return chunkId, logicalpoolId, copysetId, err
func QueryChunkCopyset(caller *cobra.Command) (uint64, uint32, uint32, *cmderror.CmdError) {
	queryCmd := NewQueryChunkCopysetCommand()
	config.AlignFlagsValue(caller, queryCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD, config.CURVEBS_OFFSET,
	})
	queryCmd.Cmd.SilenceErrors = true
	queryCmd.Cmd.SilenceUsage = true
	queryCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := queryCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsGetChunkCopyset()
		retErr.Format(err.Error())
		return 0, 0, 0, retErr
	}
	return queryCmd.ChunkId, queryCmd.LogicalpoolId, queryCmd.CopysetId, cmderror.Success()
}
