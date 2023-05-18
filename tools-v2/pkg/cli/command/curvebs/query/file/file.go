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
 * Created Date: 2022-08-26
 * Author: chengyi (Cyber-SiKu)
 */

package file

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	curveutil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	fileExample = `$ curve bs query file --path=/RecycleBin`
)

type FileCommand struct {
	basecmd.FinalCurveCmd
	allocSize *nameserver2.GetAllocatedSizeResponse
	fileInfo  *nameserver2.GetFileInfoResponse
	fileSize  *nameserver2.GetFileSizeResponse
}

var _ basecmd.FinalCurveCmdFunc = (*FileCommand)(nil) // check interface

func NewFileCommand() *cobra.Command {
	return NewQueryFileCommand().Cmd
}

func NewQueryFileCommand() *FileCommand {
	fileCmd := &FileCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "file",
			Short:   "query the file info and actual space",
			Example: fileExample,
		},
	}

	basecmd.NewFinalCurveCli(&fileCmd.FinalCurveCmd, fileCmd)
	return fileCmd
}

func (fCmd *FileCommand) AddFlags() {
	config.AddBsMdsFlagOption(fCmd.Cmd)
	config.AddRpcRetryTimesFlag(fCmd.Cmd)
	config.AddRpcTimeoutFlag(fCmd.Cmd)
	config.AddBsPathRequiredFlag(fCmd.Cmd)
	config.AddBsUserOptionFlag(fCmd.Cmd)
	config.AddBsPasswordOptionFlag(fCmd.Cmd)
}

func (fCmd *FileCommand) Init(cmd *cobra.Command, args []string) error {
	allocRes, err := GetAllocatedSize(fCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	fCmd.allocSize = allocRes

	infoRes, err := GetFileInfo(fCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	fCmd.fileInfo = infoRes
	if fCmd.fileInfo.GetFileInfo().GetFileType() ==
		nameserver2.FileType_INODE_DIRECTORY {
		// If it is a dir, calculate the file size in the directory
		// (specified by the user when creating it)
		sizeRes, err := GetFileSize(fCmd.Cmd)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		fCmd.fileSize = sizeRes
	}
	fCmd.Error = err

	return nil
}

func (fCmd *FileCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&fCmd.FinalCurveCmd, fCmd)
}

func updateByFileInfo(data *map[string]string, header *[]string, fileInfo *nameserver2.FileInfo) {
	pref := fileInfo.ProtoReflect()
	setHeaderData := func(name, value string) {
		(*data)[name] = value
		(*header) = append((*header), name)
	}
	pref.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		typeName := string(fd.Name())
		switch typeName {
		case "fileName":
			setHeaderData(curveutil.ROW_NAME, v.String())
		case "fileType":
			setHeaderData(curveutil.ROW_TYPE, nameserver2.FileType_name[int32(v.Enum())])
		case "chunkSize":
			setHeaderData(curveutil.ROW_CHUNK, humanize.IBytes(v.Uint()))
		case "segmentSize":
			setHeaderData(curveutil.ROW_SEGMENT, humanize.IBytes(v.Uint()))
		case "length":
			setHeaderData(typeName, humanize.IBytes(v.Uint()))
		case "fileStatus":
			setHeaderData(curveutil.ROW_STATUS, nameserver2.FileStatus_name[int32(v.Enum())])
		case "ctime":
			cTimeStr := time.Unix(int64(v.Uint()/1000000), 0).Format("2006-01-02 15:04:05")
			setHeaderData(curveutil.ROW_CTIME, cTimeStr)
		case "seqNum":
			setHeaderData(curveutil.ROW_SEQ, v.String())
		case "originalFullPathName":
			setHeaderData(curveutil.ROW_ORIGINAL_PATH, v.String())
		case "stripeUnit":
		case "stripeCount":
		case "throttleParams":
		case "parentId":
		default:
			setHeaderData(typeName, v.String())
		}
		return true
	})
	if fileInfo.StripeCount != nil && fileInfo.StripeUnit != nil {
		message := fmt.Sprintf("count:%d\nuint:%d",
			fileInfo.GetStripeCount(), fileInfo.GetStripeUnit())
		setHeaderData(curveutil.ROW_STRIPE, message)
	}
	if fileInfo.ThrottleParams != nil {
		message := ""
		for _, param := range fileInfo.GetThrottleParams().GetThrottleParams() {
			message += fmt.Sprintf("\ntype:%s\nlimit:%d",
				nameserver2.ThrottleType_name[int32(param.GetType())], param.GetLimit())
			if param.Burst != nil && param.BurstLength != nil {
				message += fmt.Sprintf("\nburst:%d\nburstLength:%d",
					param.GetBurst(), param.GetBurstLength())
			}
		}
		message = message[1:]
		setHeaderData(curveutil.ROW_THROTTLE, message)
	}
}

func updateByAllocSize(data *map[string]string, header *[]string, allocSize *nameserver2.GetAllocatedSizeResponse) {
	name := curveutil.ROW_ALLOC
	message := ""
	if allocSize.AllocatedSize != nil {
		message += fmt.Sprintf("size:%s", humanize.IBytes(allocSize.GetAllocatedSize()))
	}
	allocMap := allocSize.GetAllocSizeMap()
	for k, v := range allocMap {
		message += fmt.Sprintf("\nlogical pool:%d, size:%s", k, humanize.IBytes(v))
	}
	(*data)[name] = message
	(*header) = append((*header), name)
}

func updateByFileSize(data *map[string]string, header *[]string, size *nameserver2.GetFileSizeResponse) {
	if 	size != nil && size.FileSize != nil {
		name := curveutil.ROW_SIZE
		message := fmt.Sprintf("size:%s", humanize.IBytes(size.GetFileSize()))
		(*data)[name] = message
		(*header) = append((*header), name)
	}
}

func (fCmd *FileCommand) RunCommand(cmd *cobra.Command, args []string) error {
	data := make(map[string]string, 0)
	header := make([]string, 0)
	updateByFileInfo(&data, &header, fCmd.fileInfo.GetFileInfo())
	updateByAllocSize(&data, &header, fCmd.allocSize)
	updateByFileSize(&data, &header, fCmd.fileSize)
	fCmd.SetHeader(header)
	list := curveutil.Map2List(data, header)
	fCmd.TableNew.Append(list)
	fCmd.Result = map[string]interface{}{
		"info": fCmd.fileInfo,
		"alloc": fCmd.allocSize,
	}
	if fCmd.fileSize != nil {
		fCmd.Result.(map[string]interface{})["size"] = fCmd.fileSize
	}
	
	return nil
}

func (fCmd *FileCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&fCmd.FinalCurveCmd)
}
