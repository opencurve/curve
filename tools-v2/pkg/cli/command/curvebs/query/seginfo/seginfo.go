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
 * Project: CurveCli
 * Created Date: 2023-04-11
 * Author: chengyi (Cyber-SiKu)
 */

package seginfo

import (
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
)

const (
	seginfoExample = `$ curve bs query seginfo --path /pagefile`
)

type SeginfoCommand struct {
	basecmd.FinalCurveCmd
	FileInfo *nameserver2.FileInfo
}

var _ basecmd.FinalCurveCmdFunc = (*SeginfoCommand)(nil) // check interface

func NewQuerySeginfoCommand() *SeginfoCommand {
	seginfoCmd := &SeginfoCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "seginfo",
			Short:   "query the segments info of the file",
			Example: seginfoExample,
		},
	}

	basecmd.NewFinalCurveCli(&seginfoCmd.FinalCurveCmd, seginfoCmd)
	return seginfoCmd
}

func NewSeginfoCommand() *cobra.Command {
	return NewQuerySeginfoCommand().Cmd
}

func (sCmd *SeginfoCommand) AddFlags() {
	config.AddBsMdsFlagOption(sCmd.Cmd)
	config.AddRpcRetryTimesFlag(sCmd.Cmd)
	config.AddRpcTimeoutFlag(sCmd.Cmd)
	config.AddBsPathRequiredFlag(sCmd.Cmd)
}

func (sCmd *SeginfoCommand) Init(cmd *cobra.Command, args []string) error {
	fileInfoResponse, err := file.GetFileInfo(sCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	sCmd.FileInfo = fileInfoResponse.GetFileInfo()
	if (sCmd.FileInfo.GetFileType() != nameserver2.FileType_INODE_PAGEFILE &&
        sCmd.FileInfo.GetFileType() != nameserver2.FileType_INODE_CLONE_PAGEFILE) {
		filepath := config.GetBsFlagString(sCmd.Cmd, config.CURVEBS_PATH)
		return fmt.Errorf("file %s is not a pagefile", filepath)
	}

	header := []string{cobrautil.ROW_LOGICALPOOL, cobrautil.ROW_SEGMENT_SIZE, cobrautil.ROW_CHUNK_SIZE, cobrautil.ROW_START, cobrautil.ROW_COPYSET, cobrautil.ROW_CHUNK}
	sCmd.SetHeader(header)
	sCmd.TableNew.SetAutoMergeCellsByColumnIndex(
		cobrautil.GetIndexSlice(header, []string{
			cobrautil.ROW_LOGICALPOOL, cobrautil.ROW_SEGMENT_SIZE, cobrautil.ROW_CHUNK_SIZE, cobrautil.ROW_START, cobrautil.ROW_COPYSET,
			cobrautil.ROW_CHUNK,
		}),
	)
	return nil
}

func (sCmd *SeginfoCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&sCmd.FinalCurveCmd, sCmd)
}

func (sCmd *SeginfoCommand) RunCommand(cmd *cobra.Command, args []string) error {
	segmentNum := sCmd.FileInfo.GetLength() / uint64(sCmd.FileInfo.GetSegmentSize())
	segmentSize := sCmd.FileInfo.GetSegmentSize()
	var errs []*cmderror.CmdError
	var segments []*nameserver2.PageFileSegment
	config.AddBsOffsetRequiredFlag(sCmd.Cmd)
TaverSegment:
	for i := uint64(0); i < segmentNum; i++ {
		sCmd.Cmd.ParseFlags([]string{
			fmt.Sprintf("--%s", config.CURVEBS_OFFSET), fmt.Sprintf("%d", i*uint64(segmentSize)),
		})
		segmentRes, err := GetSegment(sCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS && segmentRes.GetStatusCode() != nameserver2.StatusCode_kSegmentNotAllocated {
			errs = append(errs, err)
			continue
		}
		switch segmentRes.GetStatusCode() {
		case nameserver2.StatusCode_kOK:
			segments = append(segments, segmentRes.GetPageFileSegment())
		case nameserver2.StatusCode_kFileNotExists:
			// The file is deleted during the query process, clear the segment and return 0
			segments = nil
			break TaverSegment
		default:
			continue
		}
	}
	sCmd.Error = cmderror.MergeCmdErrorExceptSuccess(errs)
	sCmd.Result = segments

	var rows []map[string]string
	for _, segment := range segments {
		for _, chunk := range segment.GetChunks() {
			row := make(map[string]string)
			row[cobrautil.ROW_LOGICALPOOL] = fmt.Sprintf("%d", segment.GetLogicalPoolID())
			row[cobrautil.ROW_SEGMENT_SIZE] = fmt.Sprintf("%d", segment.GetSegmentSize())
			row[cobrautil.ROW_CHUNK_SIZE] = fmt.Sprintf("%d", segment.GetChunkSize())
			row[cobrautil.ROW_START] = fmt.Sprintf("%d", segment.GetStartOffset())
			row[cobrautil.ROW_COPYSET] = fmt.Sprintf("%d", chunk.GetCopysetID())
			row[cobrautil.ROW_CHUNK] = fmt.Sprintf("%d", chunk.GetChunkID())
			rows = append(rows, row)
		}
	}
	list := cobrautil.ListMap2ListSortByKeys(rows, sCmd.Header, []string{
			cobrautil.ROW_LOGICALPOOL, cobrautil.ROW_SEGMENT_SIZE, cobrautil.ROW_CHUNK_SIZE, cobrautil.ROW_START, cobrautil.ROW_COPYSET,
			cobrautil.ROW_CHUNK,
	})
	sCmd.TableNew.AppendBulk(list)
	return nil
}

func (sCmd *SeginfoCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&sCmd.FinalCurveCmd)
}
