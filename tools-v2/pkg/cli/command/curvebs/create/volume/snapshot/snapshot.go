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
 * Project: tools-v2
 * Created Date: 2023-7-24
 * Author: ApiaoSamaa
 */
package snapshot

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/spf13/cobra"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
)

const (
	createSnapShotExample = `curve bs create volume snapshot --path /curvebs-file-path --user [username] [--password password]`
	// TODO: I write it as a const here
	version = "0.0.6"
)

// TODO: I delete the following code, since we don't use rpc
// type CreateSnapShotRpc struct {
// 	Info      *basecmd.Rpc
// 	Request   *nameserver2.CreateSnapShotRequest
// 	mdsClient nameserver2.CurveFSServiceClient
// }

type CreateSnapshotCommand struct {
	basecmd.FinalCurveCmd
	serverAddress []string
	timeout       time.Duration

	User     string
	File     string
	Name     string
	TaskID   string
	All      bool
	Failed   bool
	messages []string
}

type QueryParams struct {
	Action  string `json:"Action"`
	Version string `json:"Version"`
	User    string `json:"User"`
	File    string `json:"File"`
	Name    string `json:"Name"`
}

// TODO: What's Record? I didn't find it in the original python code, and it's not used in this task
type Record struct {
	UUID string `json:"UUID"`
	User string `json:"User"`
	Src  string `json:"Src"`
	File string `json:"File"`

	Result string `json:"Result"`
}

var _ basecmd.FinalCurveCmdFunc = (*CreateSnapshotCommand)(nil)

// func (gRpc *CreateSnapShotRpc) NewRpcClient(cc grpc.ClientConnInterface) {
// 	gRpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)

// }

// func (gRpc *CreateSnapShotRpc) Stub_Func(ctx context.Context) (interface{}, error) {
// 	return gRpc.mdsClient.CreateSnapShot(ctx, gRpc.Request)
// }

func (c *CreateSnapshotCommand) Init(cmd *cobra.Command, args []string) error {
	mdsAddrs, err := config.GetBsMdsAddrSlice(c.Cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	c.serverAddress = mdsAddrs
	c.timeout = config.GetFlagDuration(c.Cmd, config.HTTPTIMEOUT)
	c.User = config.GetBsFlagString(c.Cmd, config.CURVEBS_USER)
	// TODO: Not sure whther this is curvebs_path
	c.File = config.GetBsFlagString(c.Cmd, config.CURVEBS_PATH)
	// TODO: name is snapshot name! The bs.go line 204.
	c.Name = config.GetBsFlagString(c.Cmd, config.CURVEBS_NAME)

	// TODO: The pr has added this CURVEBS_TASKID to bs.go, but what's the usage of it to our task?
	// c.TaskID = config.GetBsFlagString(c.Cmd, config.CURVEBS_TASKID)
	// c.All = config.GetFlagBool(c.Cmd, config.CURVEBS_ALL)
	// c.Failed = config.GetFlagBool(c.Cmd, config.CURVEBS_FAILED)
	// c.SetHeader([]string{cobrautil.ROW_USER, cobrautil.ROW_SRC, cobrautil.ROW_TASK_ID, cobrautil.ROW_FILE, cobrautil.ROW_RESULT})
	// return nil

	// type CreateSnapshotCommand struct {
	// 	basecmd.FinalCurveCmd
	// 	serverAddress []string
	// 	timeout       time.Duration

	// 	User    	string
	// 	File    	string
	// 	Name    	string
	// 	TaskID  	string
	// 	All    		bool
	// 	Failed 		bool
	// 	messages	[]string

	// }

	// timeout := config.GetFlagDuration(c.Cmd, config.RPCTIMEOUT)
	// retrytimes := config.GetFlagInt32(c.Cmd, config.RPCRETRYTIMES)
	// TODO: not used
	// path := config.GetBsFlagString(c.Cmd, config.CURVEBS_PATH)
	// username := config.GetBsFlagString(c.Cmd, config.CURVEBS_USER)
	// password := config.GetBsFlagString(c.Cmd, config.CURVEBS_PASSWORD)
	// date, errDat := cobrautil.GetTimeofDayUs()
	// if errDat.TypeCode() != cmderror.CODE_SUCCESS {
	// 	return errDat.ToError()
	// }

	// TODO: deleted because we don't use rpc
	// createRequest := nameserver2.CreateSnapShotRequest{
	// 	FileName: &path,
	// 	Owner:    &username,
	// 	Date:     &date,
	// }

	// TODO: What's the function of following code?
	// if username == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
	// 	strSig := cobrautil.GetString2Signature(date, username)
	// 	sig := cobrautil.CalcString2Signature(strSig, password)
	// 	createRequest.Signature = &sig
	// }
	// c.Rpc = &CreateSnapShotRpc{
	// 	Info:    basecmd.NewRpc(mdsAddrs, timeout, retrytimes, "CreateSnapShot"),
	// 	Request: &createRequest,
	// }

	header := []string{cobrautil.ROW_RESULT}
	c.SetHeader(header)
	c.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		c.Header, header,
	))
	return nil
}

func (c *CreateSnapshotCommand) encodeParam(params QueryParams) string {
	values := url.Values{}
	// TODO: I changed the paramsMap according to the original python code
	paramsMap := map[string]string{
		"Action":  params.Action,
		"Version": params.Version,
		"User":    params.User,
		"File":    params.File,
		"Name":    params.Name,
	}

	for key, value := range paramsMap {
		if value != "" {
			values.Add(key, value)
		}
	}

	return values.Encode()
}
func (c *CreateSnapshotCommand) query(params QueryParams, data interface{}) error {
	encodedParams := c.encodeParam(params)

	subUri := fmt.Sprintf("/CreateSnapshotService?%s", encodedParams)

	metric := basecmd.NewMetric(c.serverAddress, subUri, c.timeout)

	result, err := basecmd.QueryMetric(metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	if err := json.Unmarshal([]byte(result), &data); err != nil {
		return err
	}

	return nil
}

func (c *CreateSnapshotCommand) RunCommand(cmd *cobra.Command, args []string) error {
	params := QueryParams{
		Action:  "CreateSnapshot",
		Version: version,
		User:    c.User,
		File:    c.File,
		Name:    c.Name,
	}

	var resp struct {
		Code      string
		Message   string
		RequestId string
		// TODO: What's Record? I didn't find it in the original python code
		TaskInfos  []*Record
		TotalCount int
	}
	err := c.query(params, &resp)
	if err != nil || resp.Code != "0" {
		fmt.Printf("create snapshot fail, error=%s\n", err)
		return nil
	}
	// TODO: Originally it's 'return resp.TaskInfos', but in this task I only need to return err
	return err

}

func (CreateCommand *CreateSnapshotCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&CreateCommand.FinalCurveCmd, CreateCommand)
}

func (CreateCommand *CreateSnapshotCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&CreateCommand.FinalCurveCmd)
}

func (c *CreateSnapshotCommand) AddFlags() {
	config.AddBsMdsFlagOption(c.Cmd)
	config.AddBsUserOptionFlag(c.Cmd)
	config.AddBsPathRequiredFlag(c.Cmd)
	config.AddBsUserOptionFlag(c.Cmd)
	config.AddBsPasswordOptionFlag(c.Cmd)
}

func NewCreateSnapShotCommand() *CreateSnapshotCommand {
	CreateCommand := &CreateSnapshotCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "snapshot",
			Short:   "create volumn snapshot in curvebs",
			Example: createSnapShotExample,
		},
	}
	basecmd.NewFinalCurveCli(&CreateCommand.FinalCurveCmd, CreateCommand)
	return CreateCommand
}

func NewSnapShotCommand() *cobra.Command {
	return NewCreateSnapShotCommand().Cmd
}

// TODO, in this function the parameter about '*nameserver2.CreateSnapShotResponse' is deleted.
// But I don't have any idea about what this function is.
func CreateFile(caller *cobra.Command) *cmderror.CmdError {
	creCmd := NewCreateSnapShotCommand()
	config.AlignFlagsValue(caller, creCmd.Cmd, []string{
		config.RPCRETRYTIMES, config.RPCTIMEOUT, config.CURVEBS_MDSADDR,
		config.CURVEBS_PATH, config.CURVEBS_USER, config.CURVEBS_PASSWORD,
	})
	creCmd.Cmd.SilenceErrors = true
	creCmd.Cmd.SilenceUsage = true
	creCmd.Cmd.SetArgs([]string{"--format", config.FORMAT_NOOUT})
	err := creCmd.Cmd.Execute()
	if err != nil {
		retErr := cmderror.ErrBsCreateFileOrDirectoryType()
		retErr.Format(err.Error())
		return retErr
	}
	return cmderror.Success()
}
