/*
 * Project: tools-v2
 * Created Date: 2023-4-6
 * Author: nanguanlin6@gmail.com
 */

package clean_recycle

import (
	"strings"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/delete/file"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/dir"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
)

const (
	cleanRecycleBinExample = `$ curve bs clean-recycle --expiredtime=1h --recycleprefix=/test`
	RECYCLEBINDIR          = "/RecycleBin"
)

// CleanRecycleCommand
type CleanRecycleCommand struct {
	basecmd.FinalCurveCmd
	recyclePrefix string
	expireTime    time.Duration
}

var _ basecmd.FinalCurveCmdFunc = (*CleanRecycleCommand)(nil) // check interface

// new CleanRecycleCommand function
func NewCleanRecycleCommand() *cobra.Command {
	crCmd := &CleanRecycleCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "clean-recycle",
			Short:   "clean recycle bin",
			Example: cleanRecycleBinExample,
		},
	}
	return basecmd.NewFinalCurveCli(&crCmd.FinalCurveCmd, crCmd)
}

// method of CleanRecycleCommand struct
func (crCmd *CleanRecycleCommand) Init(cmd *cobra.Command, args []string) error {
	crCmd.recyclePrefix = config.GetBsRecyclePrefix(crCmd.Cmd)
	crCmd.expireTime = config.GetBsExpireTime(crCmd.Cmd)
	header := []string{cobrautil.ROW_RESULT}
	crCmd.SetHeader(header)
	crCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		crCmd.Header, header,
	))
	return nil
}

func (crCmd *CleanRecycleCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&crCmd.FinalCurveCmd, crCmd)
}

func (crCmd *CleanRecycleCommand) RunCommand(cmd *cobra.Command, args []string) error {
	// Get the file infos in recycle bin
	crCmd.Cmd.Flags().Set(config.CURVEBS_PATH, RECYCLEBINDIR)
	resp, err := dir.ListDir(crCmd.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		crCmd.Error = err
		crCmd.Result = cobrautil.ROW_VALUE_FAILED
		return err.ToError()
	}

	// Define the needDelete function
	needDelete := func(fileInfo *nameserver2.FileInfo, now time.Time, expireTime time.Duration) bool {
		createTime := time.Unix(int64(fileInfo.GetCtime()/1000000), 0)
		return createTime.Add(expireTime).Before(now)
	}

	// Iterate through files and delete if necessary
	now := time.Now()

	var errs []*cmderror.CmdError
	infos := resp.GetFileInfo()
	for _, fileInfo := range infos {
		originPath := fileInfo.GetOriginalFullPathName()
		if !strings.HasPrefix(originPath, crCmd.recyclePrefix) || !needDelete(fileInfo, now, crCmd.expireTime) {
			continue
		}

		filename := RECYCLEBINDIR + "/" + fileInfo.GetFileName()
		crCmd.Cmd.Flags().Set(config.CURVEBS_PATH, filename)
		crCmd.Cmd.Flags().Set(config.CURVEBS_FORCE, cobrautil.K_STRING_TRUE)
		deleteResult, err := file.DeleteFile(crCmd.Cmd)
		if deleteResult.GetStatusCode() != nameserver2.StatusCode_kOK {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) != 0 {
		crCmd.Result = cobrautil.ROW_VALUE_FAILED
		crCmd.Error = cmderror.MergeCmdError(errs)
		return crCmd.Error.ToError()
	}

	out := make(map[string]string)
	out[cobrautil.ROW_RESULT] = cobrautil.ROW_VALUE_SUCCESS
	list := cobrautil.Map2List(out, []string{cobrautil.ROW_RESULT})
	crCmd.TableNew.Append(list)

	crCmd.Result = out
	crCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (crCmd *CleanRecycleCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&crCmd.FinalCurveCmd)
}

func (crCmd *CleanRecycleCommand) AddFlags() {
	config.AddBsMdsFlagOption(crCmd.Cmd)
	config.AddRpcRetryTimesFlag(crCmd.Cmd)
	config.AddRpcTimeoutFlag(crCmd.Cmd)
	config.AddBsUserOptionFlag(crCmd.Cmd)
	config.AddBsPasswordOptionFlag(crCmd.Cmd)

	config.AddBsForceDeleteOptionFlag(crCmd.Cmd)
	config.AddBsPathOptionFlag(crCmd.Cmd)
	config.AddBsRecyclePrefixOptionFlag(crCmd.Cmd)
	config.AddBsExpireTimeOptionFlag(crCmd.Cmd)
}
